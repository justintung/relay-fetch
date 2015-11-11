/* (C) 2007-2012 Alibaba Group Holding Limited
 *
 * Authors:
 * yinfeng.zwx@taobao.com
 * xiyu.lh@taobao.com

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */


/******BUG FIX LOG**************
2012/6/18
in my_query,forget to free connection before reconnect
*******************************/

#include "relayfetch.h"

#define MAX_TABLE_NUM 20000
schema_info_st *glob_schema[MAX_TABLE_NUM];

#define MAX_LOG_SIZE 104857600 //1024 * 1024 * 100
#define RECORDLEN 32000
#define SLEEP_TIME 50000
#define UCHAR(ptr) ((*(ptr)+256)%256)

static char slave_cmd[1000];
static setting_st   setting;
static int rf_fake_read = 0;
static long int rf_event_read = 0;
static long int rf_event_convert = 0;
static long int rf_seconds_behind = 0;
static MYSQL *mt = NULL;

typedef struct slave_error_st{
   pthread_cond_t cond;
   pthread_cond_t wait_cond;
   pthread_mutex_t wait_mutex;
   pthread_mutex_t mutex;
   char err_str[2000];
   long long int bin_pos;
   long long int trx_start_pos;
   long long int trx_err_pos;
   int           err;   
   int           file_num;
   int           fix_in_process;
   int           is_success;
} slave_error_st;

slave_error_st slave_error;

enum {
SELECT_UNPACK,
INSERT_UNPACK
};


pthread_mutex_t     rf_logfile_mutex;
pthread_mutex_t     rf_monitor_mutex;
pthread_mutex_t     slave_error_mutex;

int                 glob_kill       = 0;
reader_st           *glob_reader     = NULL;
worker              *glob_worker    = NULL;

int my_query(MYSQL* mysql, const char* query);
int get_columns_info(schema_info_st* schema, char * tb);
int test_if_wait_slave(int fd, int is_end);
void* do_reader_thread(void *);
void *do_worker_thread(void *arg);
int update_slave_status(void);
int read_event_from_relay(int fd, char *buf);
const char*  unpack_record(const char *ptr, schema_info_st * schema,  
                           char *tbName, char ** record, int type, int* error);
int find_tb_index(const char *dbname, const char *tbname);

static inline void get_curr_time(char *result) {
    time_t now = time(&now);
    struct tm tnow;
    localtime_r(&now, &tnow);
    strftime(result, 255, "%H:%M:%S", &tnow);
}

static char time_buf[256];                             
#define my_printf(fmt, ...)                         \
    pthread_mutex_lock(&rf_logfile_mutex);          \
    fflush(fp_stdout);                              \
    bzero(time_buf,256);                            \
    get_curr_time(time_buf);                        \
    printf("%s %s:%d %d : ",time_buf , __FILE__,    \
            __LINE__ , (uint)pthread_self());       \
    printf(fmt, ##__VA_ARGS__);                     \
    printf("\n");                                   \
    fflush(fp_stdout);                              \
    pthread_mutex_unlock(&rf_logfile_mutex)


int os_thread_sleep(long int  tm) 
{   
    struct timeval  t;

    t.tv_sec = tm / 1000000;
    t.tv_usec = tm % 1000000;

    select(0, NULL, NULL, NULL, &t);

    return 0;
}


int connect_mysql(MYSQL* mysql, char* db)
{
    char *host = NULL;
    int reported = 0;
   
    if (setting.port !=  0)
        host = "127.0.0.1";

    while (1) {
        if (!(mysql_real_connect(mysql, 
                        host,
                        setting.user,
                        setting.password,
                        db, 
                        setting.port, 
                        setting.socket, 
                        0))) {
            if (!reported) {
                DEBUG("Couldn't connect to server %s:%s,Error:%s,"
                        "errno:%d, sleeping 60s for reconnecting\n", 
                        setting.user,
                        setting.password,
                        mysql_error(mysql),
                        mysql_errno(mysql));
                reported = 1;
            }
            /*sleep 10s*/
            os_thread_sleep(SLEEP_TIME*200);

        } else { /*success*/
             return 0;
        }
    }

    return 0;
}

/*kill relayfetch*/
int rf_kill()
{
    FILE *fp;
    const char *path = setting.pidfile;
    if ((fp=fopen(path,"r")) == NULL) {
        DEBUG("open pid file error: %s", path);
        return -1;
    }
    char buf[60];
    bzero(buf, 60);
    if (fgets(buf, 50, fp)==NULL) {
        DEBUG("error while fgets");
        return -1;
    }
    uint pid=atoi(buf);
    printf("pid=%d KILLED\n",pid);
    if (unlink(path) != 0) {
        DEBUG("unlink pidfile error");
    }
    char command[100];
    snprintf(command, 100, "sudo kill -9 %d", pid);
    system(command);
//    kill(pid,SIGKILL);
    exit(0);
}

/*free mem before exit*/
static void rf_clean(int sig)
{
    if (unlink(setting.pidfile) != 0) {
        DEBUG("unlink pidfile error");
    }
    
    int i = 0;
    for (i=0; i<setting.n_schema; i++)
        free(glob_schema[i]);
    
    for (i=0; i<setting.n_worker; i++) {
        mysql_close(&(glob_worker[i].mysql));
    }
    
    mysql_close(&(glob_reader->mysql));

    free(glob_worker);
    free(glob_reader);
    
    exit(0);
}


int my_query(MYSQL* mysql, const char* query)
{
    assert(strncasecmp(query, "select", 6)==0
           || strncasecmp(query, "show", 4) ==0);

    int ret = 0;
    int retry_count = 0;
    int has_reconn  = 0;
    int reported  = 0;

retry:    
    if ((ret=mysql_query(mysql, query)) != 0) {
        if (!reported) {
             DEBUG("query error: %s,"  
                 "127.0.0.1,errno:%d",   
                 mysql_error(mysql), mysql_errno(mysql));
            
             DEBUG("sql=%s, goto retry", query);
             reported = 1;
        }

        if ((mysql_errno(mysql)) == 1146) /*the table does not exist*/ 
            return 1146;
        else
        if ((mysql_errno(mysql)) == 1064) /*wrong sql*/
            return 1064;
        
        /*mysql has gone away, simply set retry_count=20 to force reconnect*/
        if ((mysql_errno(mysql)) == 2006 && has_reconn == 0)   //mysql has gone away
            retry_count = 20;

        if (++retry_count < 20) {
            os_thread_sleep(SLEEP_TIME*20);
            goto retry;
        }

        if (has_reconn == 1) {
            DEBUG("error happen while excute sql.....\n");
            return mysql_errno(mysql);
        } else {
            mysql_close(mysql);
            mysql_init(mysql);
            connect_mysql(mysql, NULL);
            has_reconn  = 1;
            retry_count = 0;
            os_thread_sleep(SLEEP_TIME*20);
            goto retry;
        }
    }

    return 0;
}

/* execute the converted select statement */
int process_mysql_query(const char *query, worker *work) 
{     

    int ret = 0;
    MYSQL* mysql = &(work->mysql);
    ret = my_query(mysql, query);
    
    if (ret != 0)
        return ret;

    MYSQL_RES *res = NULL;

    res = mysql_store_result(mysql);
    if (res != NULL) 
        mysql_free_result(res);
    
    return ret;
}

/*record a table information in glob_schema array*/
int record_table_info(char *db, char *tb)
{
    int i = 0;
    /*if using table partition rule (see discribtion of setting_st)*/
    if (setting.is_part) {
        while (i < setting.n_schema) {
            schema_info_st* info = glob_schema[i]; 
            if (strncmp(tb, info->tb, strlen(info->tb)) == 0
                    && strcmp(db, info->db) == 0
                    && (tb[strlen(info->tb)] == '\0' || isdigit(tb[strlen(info->tb)]))) {
                info->tb_num++; 
                return -1;
            }

            i++;
        }
    }

    i = setting.n_schema;
    glob_schema[i] = (schema_info_st*)malloc(sizeof(schema_info_st));
    bzero(glob_schema[i], sizeof(schema_info_st));

    if (setting.is_part) { 
        char *p1;
        char *p2;
        p1 = p2 = NULL;
        p1 = tb;
        while ((p1 = strstr(p1, "_")) != NULL) {
            p2 = p1;
            p1++;
        }

        if (p2 != NULL 
                && p2[1] != '\0' && isdigit(p2[1]))
            snprintf(glob_schema[i]->tb, p2-tb+2, "%s", tb);
        else
            snprintf(glob_schema[i]->tb, 200, "%s", tb);
    }
    else
        snprintf(glob_schema[i]->tb, 200, "%s", tb);

    snprintf(glob_schema[i]->db, 200, "%s", db);
    glob_schema[i]->group    = i;
    glob_schema[i]->tb_num   = 1;
    glob_schema[i]->n_modify = 0;

    int ret = get_columns_info(glob_schema[i], tb);
    if (ret == -1) {
        free(glob_schema[i]);
        return -1;  //just ignore this table
    }
    setting.n_schema++;
    
    return i;
}


int init_reader()
{
    setting.n_schema = 0; /* clean up schema number */

    MYSQL* m_db;
    MYSQL* m_tb;

    m_db = mysql_init(NULL);
    m_tb = mysql_init(NULL);
    connect_mysql(m_db, NULL);
    connect_mysql(m_tb, NULL);

    int ret = 0;

    char sql[500];
    snprintf(sql, 500, "show databases");

    ret = my_query(m_db, sql);
    
    if (ret != 0) {
       DEBUG("error happen while try to get table schemas,so exit..");
       exit(1);
    }

    MYSQL_RES *res_db = NULL;
    res_db = mysql_store_result(m_db);

    MYSQL_ROW row;
    int begin = -1;
    int end   = -1;
    while ((row = mysql_fetch_row(res_db)) != NULL) {
        if (strcasecmp(row[0], "information_schema") == 0
                || strcasecmp(row[0], "mysql") == 0
                || strcasecmp(row[0], "performance_schema") == 0
                || strcasecmp(row[0], "test") == 0)
            continue;

        bzero(sql, 500);
        snprintf(sql, 500, "show tables in %s", row[0]);
        ret = my_query(m_tb, sql);
        if (ret != 0) {
            DEBUG("error happen while try to get table schemas,so exit..");
            exit(1);
        } 
        MYSQL_RES *res_tb = NULL;
        res_tb = mysql_store_result(m_tb);
        MYSQL_ROW row_tb;
        begin = -1;
        end   = -1;
        while ((row_tb = mysql_fetch_row(res_tb)) != NULL) {
            ret = record_table_info(row[0], row_tb[0]);
            if (begin == -1 && ret != -1) 
                begin = ret;

            if (ret != -1)
                end = ret;
        }

       /*group glob_schema array by db*/ 
        if (begin != -1)
            glob_schema[begin]->group = end;

        if (res_tb != NULL)
           mysql_free_result(res_tb);
    }
   
    mysql_free_result(res_db);
   
   /*init reader thread*/
    glob_reader = (reader_st*)malloc(sizeof(reader_st));

    bzero(sql, 500);
    snprintf(sql, 500, "show variables like 'relay_log'");
    
    ret = my_query(m_db, sql);
    if (ret != 0) {
       DEBUG("error happen while try to get table schemas,so exit..");
       exit(1);
    }

    res_db = mysql_store_result(m_db);
    
    if ((row = mysql_fetch_row(res_db)) != NULL){ 
        snprintf(glob_reader->path, 500, "%s", row[1]);
        char *ptr  = NULL;
        char *ptr2 = NULL;
        ptr = strstr(glob_reader->path, ".");
        while (ptr != NULL) {
           ptr2 = strstr(ptr+1, "/");
           
           if (ptr2 != NULL)
               ptr = strstr(ptr+1, ".");
           else {
               *ptr = '\0';
               break;
           }

        }

     }   
    else {
        DEBUG("error happen while try to show var like relay_log, exit...");
        exit(1);
    }
 
    bzero(sql, 500);
    snprintf(sql, 500, "show variables like 'hostname'");
    
    ret = my_query(m_db, sql);
    if (ret != 0) {
       DEBUG("error happen while try to get table schemas,so exit..");
       exit(1);
    }

    res_db = mysql_store_result(m_db);
    
    if ((row = mysql_fetch_row(res_db)) != NULL) 
        snprintf(glob_reader->host, 200, "%s", row[1]);
    else {
        DEBUG("error happen while try to show var like hostname, exit...");
        exit(1);
    }
    
    glob_reader->slaveNum = 0;
    glob_reader->slavePos = 0;
    glob_reader->relayNum = 0;
    glob_reader->relayPos = 0;
    
    mysql_init(&(glob_reader->mysql));
    connect_mysql(&(glob_reader->mysql), NULL);
    /*get server version*/
    glob_reader->version = mysql_get_server_version(&(glob_reader->mysql));
   
    update_slave_status();
    
    glob_reader->relayNum = glob_reader->slaveNum;

    /*relay log starts from 150 in 5.5 and 149 in 5.1*/
    if (glob_reader->version>=50500) 
        glob_reader->def_pos = 150;
    else
        glob_reader->def_pos = 149;

    int relayPos = glob_reader->slavePos;
    if (relayPos<glob_reader->def_pos)
        relayPos = glob_reader->def_pos;
    
    glob_reader->relayPos = relayPos;

    mysql_free_result(res_db);
    mysql_close(m_db);
    mysql_close(m_tb);

    return 0;
}

int create_thread(pthread_t* tid, void* (*func) (void *), void * args)
{
    pthread_attr_t attr;
    int err;
    sigset_t mask, omask;

    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGQUIT);
    sigaddset(&mask, SIGHUP);
    sigaddset(&mask, SIGPIPE); // ignore SIGPIPE broken

    assert(pthread_sigmask(SIG_SETMASK, &mask, &omask) == 0);

    if (pthread_attr_init(&attr) != 0) {
        DEBUG("pthread_attr_init");
        return -1;
    }

    if (pthread_attr_setstacksize(&attr, THREAD_STACK_SIZE) != 0) {
        DEBUG("pthread_attr_setsatcksize error");
        return -1;
    }

    if ((err = pthread_create(tid, &attr, func, args)) 
            != 0) {
        DEBUG("unable to create  thread :%s", strerror(err));
        return -1;
    }

    return 0;
}


int connect_monitordb()
{
    int reported = 0;

    while(1) {
        if (!(mysql_real_connect(mt, 
                        "127.0.0.1",
                        "monitor",
                        "monitor", 
                        "monitordb", 
                        3306, 
                        NULL, 
                        0))) {
            if (!reported) { 
                DEBUG("Couldn't connect to server ,Error:%s,"
                        "errno:%d, sleeping 60s for reconnecting\n", 
                        mysql_error(mt),
                        mysql_errno(mt));
                reported = 1;
            }
            /*sleep 10s*/
            os_thread_sleep(SLEEP_TIME*200);

        } else { /*success*/
            return 0;
        }
    }
}


int  insert_monitordb(char *sql)
{
    int ret = 0;
    int reported = 0;
    int trycount = 0;
    int hasconn  = 0;
    while (1) {    
        if ((ret=mysql_query(mt, sql)) != 0) {
            if (!reported) {
                DEBUG("query error: %s,"  
                        "errno:%d",   
                        mysql_error(mt), mysql_errno(mt));

                DEBUG("sql=%s, goto retry", sql);
                reported = 1;
            }

            trycount++;
            if (trycount <= 10) {
                os_thread_sleep(SLEEP_TIME*10);
                continue;
            }

            if (hasconn)
                return -1;
            else {
                mysql_init(mt);
                connect_monitordb();
                hasconn  = 1;
                trycount = 0;
                os_thread_sleep(SLEEP_TIME*20);
                continue; 
            } 
         } else
         return 0;
    }
}

unsigned long get_file_size (const char *filename)
{
  struct stat buf;
  if (stat(filename, &buf) < 0)
      return 0;

  return (unsigned long)buf.st_size;    
}

void check_logfile_size()
{
    /*check rf.log*/
    const char *path =  setting.logfile;
    unsigned long length = get_file_size(path);
    if (length>=MAX_LOG_SIZE)
        truncate(path, 0);

    /*check monitor.log*/
    char mpath[PATH_MAX];
    bzero(mpath, PATH_MAX);
    snprintf(mpath, PATH_MAX, "%s/monitor.log", setting.logdir);
    
    length = get_file_size(mpath);
    if (length>=MAX_LOG_SIZE)
        truncate(mpath, 0);

    return;    
}


void *do_monitor_thread(void *arg)
{
    char path[PATH_MAX];
    bzero(path, PATH_MAX);
    snprintf(path, PATH_MAX, "%s/monitor.log", setting.logdir);
    FILE* fp = fopen(path, "a");
    if (NULL == fp) {
        DEBUG("open monitor.log failed");
        exit(1);
    } 
    
    mt = mysql_init(NULL);
    connect_monitordb();
    char sql[500];

    char cur_time[256];
    int loops = 0;

    while(setting.running) {
        sleep(60);
        get_curr_time(cur_time);
        pthread_mutex_lock(&rf_monitor_mutex);
        fprintf(fp, "##################Monitor Info################\n");
        fprintf(fp, "Current:%s\n", cur_time);
        fprintf(fp, "Fake Read:%d\n", rf_fake_read);
        fprintf(fp, "In 60s : events read:%ld; events convert:%ld\n",\
                rf_event_read, rf_event_convert);
        fprintf(fp, "Seconds Behind:%ld\n", rf_seconds_behind);
        fprintf(fp, "#####################END######################\n");
        fflush(fp);
        bzero(cur_time, 256);
        bzero(sql, 500);
        snprintf(sql, 500, "insert into rf_mt values ('%s', NULL, %ld, %ld, %ld, %d)",
                                          glob_reader->host, rf_event_read,
                                          rf_event_convert, rf_seconds_behind,
                                          rf_fake_read);
        rf_event_read    = 0;
        rf_event_convert = 0;
        pthread_mutex_unlock(&rf_monitor_mutex);
        insert_monitordb(sql);

        if ((++loops)>=10) {
            pthread_mutex_lock(&rf_logfile_mutex);
            check_logfile_size(); 
            pthread_mutex_unlock(&rf_logfile_mutex);
            loops = 0;    
        }

    }

    fclose(fp);
    pthread_exit(0); 
}


void *do_worker_thread(void *arg) 
{
    worker* curWorker = (worker *)arg;
    int slot = curWorker->index;
    const char *data;
    struct timeval tv_start, tv_end;
    unsigned long long tv_diff = 0;
    int count = 0;
    while (setting.running) { 
        while (sem_wait(&sems_empty[slot]) == -1 &&
                    EINTR == errno);

        data = buffer[slot][node[slot].head++];
        
        gettimeofday(&tv_start, NULL);
        
        process_mysql_query(data, curWorker);
        
        gettimeofday(&tv_end, NULL);
        if (tv_end.tv_usec < tv_start.tv_usec) {            
            tv_diff += (1000000 + tv_end.tv_usec - tv_start.tv_usec);
            tv_diff += (tv_end.tv_sec-1 - tv_start.tv_sec)*1000000; 

        } else {
            tv_diff += (tv_end.tv_usec - tv_start.tv_usec);
            tv_diff += (tv_end.tv_sec - tv_start.tv_sec)*1000000; 
        }

        memset(buffer[slot][node[slot].head-1], 0, ITEM_LEN);

        if (node[slot].head >= SLOT_ITEMS)
            node[slot].head %= SLOT_ITEMS;
        
        if (++count >= N_STATS_ITV && 
                count % N_STATS_ITV == 0) {
            DEBUG("STATS: slot %d items=%d loops=%d rps=%llu latency=%llu", 
                    slot, 
                    abs(node[slot].head-node[slot].tail),
                    ++(node[slot].loops),
                    1000000*N_STATS_ITV/tv_diff,
                    tv_diff/N_STATS_ITV
                 );
             count = 0;
             tv_diff = 0;
         }

         sem_post(&sems_full[slot]);
    }

    pthread_exit(0);
}

/* one thread access a queue 
 * it's thread safe 
 */
void put_data_to_buffer(int slot, char **data, time_t when)
{   
    int i= 0;
    while (i<MAX_UK && data[i][0] != '\0') {
      pthread_mutex_lock(&rf_monitor_mutex);
      rf_event_convert++ ;
      pthread_mutex_unlock(&rf_monitor_mutex);


      while (sem_wait(&sems_full[slot]) == -1 &&
          EINTR == errno );

      snprintf(buffer[slot][node[slot].tail++], ITEM_LEN, "%s", data[i]);

      if (node[slot].tail >= SLOT_ITEMS)
        node[slot].tail %= SLOT_ITEMS;

      sem_post(&sems_empty[slot]);

      i++;
    }
}

/*return index of first table with dbname in glob_schema array*/
int find_db_index(char *dbname)
{
    int i=0;
    schema_info_st *info;

    while (i<setting.n_schema) {
        info = glob_schema[i];
        if (strcmp(dbname, info->db) == 0) {
            return i;
        } else {
            if (glob_schema[i]->group == (setting.n_schema-1))
                return -1;
            i = glob_schema[i]->group+1;
        } 
    }

    return -1;
}

/* return index of (db,tb) if found in glob_schema array,
 * else return -1
 */
int find_tb_index(const char *dbname, const char *tbname)
{
    int i=0,j=0;
    schema_info_st *info;

    while (i<setting.n_schema) {
        info = glob_schema[i];
        if (strcasecmp(dbname, info->db) == 0) {
            for (j=i; j<=glob_schema[i]->group; j++) {
                info = glob_schema[j];
                if (setting.is_part) {
                    if ( strncasecmp(tbname, info->tb, strlen(info->tb)) == 0 && 
                            (tbname[strlen(info->tb)] == '\0' || 
                             isdigit(tbname[strlen(info->tb)]))
                       )
                        return j;
                } else {
                    if (strcasecmp(tbname, info->tb) == 0)
                        return j;
                }
            }
            return -1;
        } else {
            i = glob_schema[i]->group+1;
        } 
    }

    return -1;
}


int get_columns_info(schema_info_st* schema, char * tb)
{
    MYSQL* mysql;
    MYSQL* my_set;
    MYSQL* my_is;

    mysql  = mysql_init(NULL);
    my_set = mysql_init(NULL);
    my_is  = mysql_init(NULL);

    connect_mysql(mysql,  schema->db);
    connect_mysql(my_set, schema->db);
    connect_mysql(my_is, schema->db);


    char sql[1000];
    char sql_set[500];
    char sql_is[1000];

    sprintf(sql, "select * from  %s limit 1", tb);/*to get column type*/
    sprintf(sql_set, "show columns from %s", tb); /*column type detail*/
    sprintf(sql_is, "select CHARACTER_OCTET_LENGTH from "                      /*column length*/
            "information_schema.columns where table_schema=\'%s\' "
            " and table_name = \'%s\'", schema->db, tb);
    
    int ret     = 0;
    int ret_set = 0;
    int ret_is  = 0;

    ret = my_query(mysql, sql);
    if (ret != 0) {
        mysql_close(mysql);
        mysql_close(my_set);
        mysql_close(my_is);
        return -1;
    }

    ret_set = my_query(my_set, sql_set);
    ret_is  = my_query(my_is, sql_is);
 
    if (ret_is !=0 || ret_set != 0 ) {
        DEBUG("error happen while try to get info from i_s");
		mysql_close(mysql);
        mysql_close(my_set);
        mysql_close(my_is);
        return -1;
	}

    MYSQL_RES *res     = NULL;
    MYSQL_RES *res_set = NULL;
    MYSQL_RES *res_is  = NULL;

    res     = mysql_store_result(mysql);
    res_set = mysql_store_result(my_set);
    res_is  = mysql_store_result(my_is);

    if (res == NULL || res_set == NULL || res_is == NULL ) {
        DEBUG("some error happen while setting cols...so exit\n");
        mysql_close(mysql);
        mysql_close(my_set);
        mysql_close(my_is);
        return -1;
    }

    MYSQL_ROW row;
    MYSQL_ROW row_is;

    MYSQL_FIELD  *field;
    int i = 0;
    schema->has_pk = 0;

    while ((field = mysql_fetch_field(res)) 
        && (row = mysql_fetch_row(res_set)) 
        && (row_is = mysql_fetch_row(res_is))) {
        schema->column[i].id = i;
        
        snprintf(schema->column[i].col_name, sizeof(schema->column[i].col_name), "%s", field->org_name);
        
        schema->column[i].col_type = field->type;
        if (row_is[0] != NULL)
            schema->column[i].length   = atoll(row_is[0]);
        else
            schema->column[i].length   = field->length;
        schema->column[i].decimals = field->decimals;
        
        if (strcasestr(row[1], "set") != NULL || strcasestr(row[1], "enum") != NULL) {
            if (strcasestr(row[1],"set") != NULL)
                schema->column[i].col_type = MYSQL_TYPE_SET;
            else
                schema->column[i].col_type = MYSQL_TYPE_ENUM;

            char *ptr = NULL;
            char *p   = NULL;
            p = row[1];
            int k = 0;
            while(p != NULL && (ptr = strstr(p, "\',\'")) != NULL){
                k++;
                p = ptr+3 ;
            }

            schema->column[i].length = k+1;
        }

        if (row[1] && strcasestr(row[1], "unsigned"))
            schema->column[i].is_sign = 0;
        else
            schema->column[i].is_sign = 1;

        if ( row[3]  && strcasestr(row[3], "PRI")) {
            schema->column[i].is_pk_or_uk = 1;
            schema->has_pk = 1;
        }
        else
            schema->column[i].is_pk_or_uk = 0;

        i++;
    }

    schema->cols = i;
    mysql_free_result(res_is);
    mysql_free_result(res);
    mysql_free_result(res_set);
    mysql_close(my_is);
    mysql_close(my_set);

/*read index info of this table.*/
    char sql_index[2000];
    sprintf(sql_index, "select index_info.*, column_info.ORDINAL_POSITION from"
                       " (select index_name, column_name, seq_in_index from information_schema.statistics "
                       "where table_name='%s' and table_schema='%s' and non_unique=0) as index_info "
                       "left join ("
                       "select ORDINAL_POSITION, COLUMN_NAME from information_schema.columns"
                       " where table_name='%s' and table_schema='%s') as column_info"
                       " on index_info.column_name=column_info.column_name"
                       " order by index_info.index_name,index_info.seq_in_index",tb, schema->db, tb, schema->db);
    
    char sql_index_num[500];
    sprintf(sql_index_num, "select count(distinct(index_name)) from information_schema.statistics where table_name='%s' and table_schema='%s' and non_unique=0", tb, schema->db);
    
    ret= my_query(mysql, sql_index_num);
    schema->uk_num= 0;
    if (!ret && (res= mysql_store_result(mysql))!= NULL) {
      MYSQL_ROW row;
      row=mysql_fetch_row(res);
      schema->uk_num = atol(row[0]);
      mysql_free_result(res);
    }
   
    if (schema->uk_num == 0)
      goto end;
    
    assert(schema->uk_num<=MAX_UK);
    
    ret= my_query(mysql, sql_index);
    if (!ret && (res= mysql_store_result(mysql))!= NULL) {
      MYSQL_ROW row;
      char tmp_name[100];
      bzero(tmp_name, 100);
      bzero(schema->uk_ct,  MAX_UK);
      bzero(schema->uk_bit, MAX_UK*100);
      
      int index=-1;
      while ((row=mysql_fetch_row(res)) != NULL) {
        if (tmp_name[0] == '\0' || strcasecmp(tmp_name, row[0]) != 0) {
           sprintf(tmp_name, "%s", row[0]);
           index++; 
        }
        
        schema->uk_bit[index][schema->uk_ct[index]]= atol(row[3]);
        schema->uk_ct[index]++;
        schema->column[atol(row[3])-1].is_pk_or_uk= 1;
      }

      mysql_free_result(res);
    }

end:
    mysql_close(mysql);
    return 0;
}


/*init decimal structure*/
int init_dec(decimal_t *dec)
{
    int i = 0; 
    for (i =0 ; i<DECIMAL_MAX_PRECISION; i++) 
        dec->buf[i] = 0; 

    dec->sign = 0; 
    dec->intg = 0; 
    dec->frac = 0; 

    return 0;
}


int decimal_bin_size(int precision, int scale)
{
    int intg=precision-scale;
    int intg0=intg/DIG_PER_DEC1;
    int frac0=scale/DIG_PER_DEC1;
    int intg0x=intg-intg0*DIG_PER_DEC1;
    int    frac0x=scale-frac0*DIG_PER_DEC1;

    return intg0*sizeof(dec1)+dig2bytes[intg0x]+
                       frac0*sizeof(dec1)+dig2bytes[frac0x];
}


int bin2decimal(const uchar *from, decimal_t *to, int precision, int scale)
{
  int error=E_DEC_OK, intg=precision-scale,
      intg0=intg/DIG_PER_DEC1, frac0=scale/DIG_PER_DEC1,
      intg0x=intg-intg0*DIG_PER_DEC1, frac0x=scale-frac0*DIG_PER_DEC1,
      intg1=intg0+(intg0x>0), frac1=frac0+(frac0x>0);
  dec1 *buf=to->buf, mask=(*from & 0x80) ? 0 : -1;
  const uchar *stop;
  uchar *d_copy;
  int bin_size= decimal_bin_size(precision, scale);

  d_copy= (uchar*) my_alloca(bin_size);
  memcpy(d_copy, from, bin_size);
  d_copy[0]^= 0x80;
  from= d_copy;

  FIX_INTG_FRAC_ERROR(to->len, intg1, frac1, error);
  if (unlikely(error))
  {
    if (intg1 < intg0+(intg0x>0))
    {
      from+=dig2bytes[intg0x]+sizeof(dec1)*(intg0-intg1);
      frac0=frac0x=intg0x=0;
      intg0=intg1;
    }
    else
    {
      frac0x=0;
      frac0=frac1;
    }
  }

  to->sign=(mask != 0);
  to->intg=intg0*DIG_PER_DEC1+intg0x;
  to->frac=frac0*DIG_PER_DEC1+frac0x;

  if (intg0x)
  {
    int i=dig2bytes[intg0x];
    dec1 UNINIT_VAR(x);
    switch (i)
    {
      case 1: x=mi_sint1korr(from); break;
      case 2: x=mi_sint2korr(from); break;
      case 3: x=mi_sint3korr(from); break;
      case 4: x=mi_sint4korr(from); break;
      default: DBUG_ASSERT(0);
    }
    from+=i;
    *buf=x ^ mask;
    if (((ulonglong)*buf) >= (ulonglong) powers10[intg0x+1])
      goto err;
    if (buf > to->buf || *buf != 0)
      buf++;
    else
      to->intg-=intg0x;
  }
  for (stop=from+intg0*sizeof(dec1); from < stop; from+=sizeof(dec1))
  {
    DBUG_ASSERT(sizeof(dec1) == 4);
    *buf=mi_sint4korr(from) ^ mask;
    if (((uint32)*buf) > DIG_MAX)
      goto err;
    if (buf > to->buf || *buf != 0)
      buf++;
    else
      to->intg-=DIG_PER_DEC1;
  }
  DBUG_ASSERT(to->intg >=0);
  for (stop=from+frac0*sizeof(dec1); from < stop; from+=sizeof(dec1))
  {
    DBUG_ASSERT(sizeof(dec1) == 4);
    *buf=mi_sint4korr(from) ^ mask;
    if (((uint32)*buf) > DIG_MAX)
      goto err;
    buf++;
  }
  if (frac0x)
  {
    int i=dig2bytes[frac0x];
    dec1 UNINIT_VAR(x);
    switch (i)
    {
      case 1: x=mi_sint1korr(from); break;
      case 2: x=mi_sint2korr(from); break;
      case 3: x=mi_sint3korr(from); break;
      case 4: x=mi_sint4korr(from); break;
      default: DBUG_ASSERT(0);
    }
    *buf=(x ^ mask) * powers10[DIG_PER_DEC1 - frac0x];
    if (((uint32)*buf) > DIG_MAX)
      goto err;
    buf++;
  }
  my_afree(d_copy);
  return error;

err:
  my_afree(d_copy);
  decimal_make_zero(((decimal_t*) to));
  return(E_DEC_BAD_NUM);
}


static dec1 *remove_leading_zeroes(const decimal_t *from, int *intg_result)
{
  int intg= from->intg, i;
  dec1 *buf0= from->buf;
  i= ((intg - 1) % DIG_PER_DEC1) + 1;
  while (intg > 0 && *buf0 == 0)
  {
    intg-= i;
    i= DIG_PER_DEC1;
    buf0++;
  }
  if (intg > 0)
  {
    for (i= (intg - 1) % DIG_PER_DEC1; *buf0 < powers10[i--]; intg--) ;
    DBUG_ASSERT(intg > 0);
  }
  else
    intg=0;
  *intg_result= intg;
  return buf0;
}


int decimal2string(const decimal_t *from, char *to, int *to_len,
                   int fixed_precision, int fixed_decimals,
                   char filler)
{
  int len, intg, frac= from->frac, i, intg_len, frac_len, fill;
  /* number digits before decimal point */
  int fixed_intg= (fixed_precision ?
                   (fixed_precision - fixed_decimals) : 0);
  int error=E_DEC_OK;
  char *s=to;
  dec1 *buf, *buf0=from->buf, tmp;

  DBUG_ASSERT(*to_len >= 2+from->sign);

  /* removing leading zeroes */
  buf0= remove_leading_zeroes(from, &intg);
  if (unlikely(intg+frac==0))
  {
    intg=1;
    tmp=0;
    buf0=&tmp;
  }

  if (!(intg_len= fixed_precision ? fixed_intg : intg))
    intg_len= 1;
  frac_len= fixed_precision ? fixed_decimals : frac;
  len= from->sign + intg_len + test(frac) + frac_len;
  if (fixed_precision)
  {
    if (frac > fixed_decimals)
    {
      error= E_DEC_TRUNCATED;
      frac= fixed_decimals;
    }
    if (intg > fixed_intg)
    {
      error= E_DEC_OVERFLOW;
      intg= fixed_intg;
    }
  }
  else if (unlikely(len > --*to_len)) /* reserve one byte for \0 */
  {
    int j= len-*to_len;
    error= (frac && j <= frac + 1) ? E_DEC_TRUNCATED : E_DEC_OVERFLOW;
    if (frac && j >= frac + 1) j--;
    if (j > frac)
    {
      intg-= j-frac;
      frac= 0;
    }
    else
      frac-=j;
    len= from->sign + intg_len + test(frac) + frac_len;
  }
  *to_len=len;
  s[len]=0;

  if (from->sign)
    *s++='-';

  if (frac)
  {
    char *s1= s + intg_len;
    fill= frac_len - frac;
    buf=buf0+ROUND_UP(intg);
    *s1++='.';
    for (; frac>0; frac-=DIG_PER_DEC1)
    {
      dec1 x=*buf++;
      for (i=min(frac, DIG_PER_DEC1); i; i--)
      {
        dec1 y=x/DIG_MASK;
        *s1++='0'+(uchar)y;
        x-=y*DIG_MASK;
        x*=10;
      }
    }
    for(; fill; fill--)
      *s1++=filler;
  }

  fill= intg_len - intg;
  if (intg == 0)
    fill--; /* symbol 0 before digital point */
  for(; fill; fill--)
    *s++=filler;
  if (intg)
  {
    s+=intg;
    for (buf=buf0+ROUND_UP(intg); intg>0; intg-=DIG_PER_DEC1)
    {
      dec1 x=*--buf;
      for (i=min(intg, DIG_PER_DEC1); i; i--)
      {
        dec1 y=x/10;
        *--s='0'+(uchar)(x-y*10);
        x=y;
      }
    }
  }
  else
    *s= '0';
  return error;
}


uint year_2000_handling(uint year)
{
  if ((year=year+1900) < 1970)
      year+=100;
    return year;
}


/*calculate bytes to store string length*/
int get_byte_by_length(long long length)
{
    int i = 0;
    
    while((length/=256) != 0)
        i++;

    return (i+1);
}


#define COL_LEN 4000
char pk_val[100][COL_LEN];
const char*  unpack_record(const char *ptr, schema_info_st * schema,  
                           char *tbName, char ** record, int type, int* error)
{
    if (schema->cols == 0) {
        /*some error must happen because schema->cols has been inited in function read_tb_info */
        DEBUG("error happen, and exit now......");
        exit(1);
        //get_columns_info(schema, tbName);
    }
    
    bzero(pk_val, 100*COL_LEN);
    decimal_t dec; 
    bzero(&dec, sizeof(dec));
    decimal_digit_t dec_buf[DECIMAL_MAX_PRECISION];
    dec.buf = dec_buf;
    char dec_str[200];

    char cols = schema->cols;
    int bits = (cols+7)/8;
    const char *row_begin = ptr;
    int i = 0;
    ptr += bits;
   
    char rec_uk[RECORDLEN]; 
    bzero(rec_uk, RECORDLEN);

    int k = 0;
    char str[RECORDLEN];
    long int d_int;
    float d_f;
    double d_d;
    int byt_len = 1;
    unsigned int length  = 0;
    unsigned long long day_val = 0;
    int prec     = 0;
    int decs     = 0;
    int prec_len = 0;
    int bt_n     = 0;
    int j        = 0;
    int is_null  = 0;
    int byt      = 0;
    int by_nu    = 0;
    long long si = 0;
    unsigned long long usi = 0;
    int found   = 0;
    int is_sign = 0;
    int is_pk   = 0;
    long long pk   = 0;
    int str_len    = 0;
    char tmpf[320];
    int count=0;
    while (k < cols) {
        byt    = (k+8)/8 - 1;
        by_nu  = k%8; 
        is_null = ((UCHAR(row_begin + byt)) & (1 << by_nu));

        if (is_null == 0) {
            is_pk   = schema->column[k].is_pk_or_uk;
            is_sign = schema->column[k].is_sign;
            found = is_pk;
            switch(schema->column[k].col_type){
                case MYSQL_TYPE_LONG:         //int
                    if (is_pk || type == INSERT_UNPACK) { 
                        if (is_sign)
                            pk = sint4korr(ptr);
                        else
                            pk = uint4korr(ptr);

                        snprintf(pk_val[k], COL_LEN, "%s = %lld ", 
                                schema->column[k].col_name, pk);
                    }
                    
                    ptr+=4;
                    break;
                case MYSQL_TYPE_TINY:   //tinyint, 1byte
                    if (is_pk || type == INSERT_UNPACK) {
                        if (is_sign)
                            d_int = (char)ptr[0];
                        else
                            d_int = (unsigned char)ptr[0];

                        snprintf(pk_val[k], COL_LEN, "%s = %ld ", 
                                schema->column[k].col_name, d_int);
                    }  
                    ptr++;
                    break;
                case MYSQL_TYPE_SHORT:  //smallint,2byte
                    if (is_pk || type == INSERT_UNPACK) {
                        if (is_sign)
                            d_int = (int32)sint2korr(ptr);
                        else
                            d_int = (int32)uint2korr(ptr);

                        snprintf(pk_val[k], COL_LEN, "%s = %ld ", 
                                schema->column[k].col_name, d_int);

                     }

                    ptr+=2;
                    break;
                case MYSQL_TYPE_INT24: //MEDIUMINT
                    if (is_pk || type == INSERT_UNPACK) {
                        if (is_sign)
                            d_int = sint3korr(ptr);
                        else
                            d_int = uint3korr(ptr);

                        snprintf(pk_val[k], COL_LEN, "%s = %ld ", 
                                schema->column[k].col_name, d_int);

                    }

                    ptr+=3;
                    break;
                case MYSQL_TYPE_LONGLONG: //bigint
                    if (is_pk || type == INSERT_UNPACK) {
                        if (is_sign) {
                            si = sint8korr(ptr);
                            snprintf(pk_val[k], COL_LEN, "%s = %lld ", 
                                    schema->column[k].col_name, si);
                       }
                        else {
                            usi = uint8korr(ptr);
                            snprintf(pk_val[k], COL_LEN, "%s = %lld ", 
                                    schema->column[k].col_name, usi);
                       }
                    } 

                    ptr+=8;
                    break;
                case MYSQL_TYPE_DECIMAL:  //decimal or numeric
                    break;
                case MYSQL_TYPE_NEWDECIMAL:
                    init_dec(&dec);
                    if (schema->column[k].decimals == 0)
                        prec = schema->column[k].length-1;
                    else
                        prec = schema->column[k].length-2;

                    decs = schema->column[k].decimals; 

                    if (is_pk || type == INSERT_UNPACK) { 
                        dec.len = prec;
                        bzero(dec_str,201);
                        bin2decimal((const uchar*)ptr, &dec, prec, decs);

                        str_len = 200;
                        decimal2string(&dec, dec_str, &str_len, 0, 0, 0);

                        snprintf(pk_val[k], COL_LEN, "%s = %s ", 
                                schema->column[k].col_name, dec_str);

                    }

                    prec_len = decimal_bin_size(prec, decs);
                    ptr+=prec_len;
                    break;
                case MYSQL_TYPE_FLOAT:   //float
                    if (is_pk || type == INSERT_UNPACK) {
                        float4get(d_f, ptr);
                        bzero(tmpf,sizeof(tmpf));
                        sprintf(tmpf, "%-20g", (double) d_f);
                        snprintf(pk_val[k], COL_LEN, "%s like %s", 
                                schema->column[k].col_name, tmpf);
                   }

                    ptr+=4;
                    break;
                case MYSQL_TYPE_DOUBLE: //double
                    if (is_pk || type == INSERT_UNPACK) {
                        float8get(d_d, ptr);
                        bzero(tmpf, sizeof(tmpf));
                        sprintf(tmpf, "%-20g", d_d);
                        snprintf(pk_val[k], COL_LEN, "%s = %s ", 
                                schema->column[k].col_name,tmpf); 
                    }

                    ptr+=8;
                    break;
                case MYSQL_TYPE_BIT: //bit
                    byt_len = (schema->column[k].length)%8==0?
                        (schema->column[k].length)/8 :
                        ((schema->column[k].length)/8 + 1);
                    bt_n = byt_len;

                    if (is_pk || type == INSERT_UNPACK) {
                        i = 1; 
                        d_int = 0; 
                        while (byt_len > 0){    
                            d_int += UCHAR(ptr + byt_len-1) *i;
                            i = i*256;
                            byt_len--;
                        }
                        snprintf(pk_val[k], COL_LEN, "%s = %ld ", 
                                schema->column[k].col_name, d_int);
                   }

                    ptr+=bt_n;
                    break;
                case MYSQL_TYPE_SET: //set
                    bt_n = (schema->column[k].length -1)/8;
                    switch(bt_n){
                        case 0:byt_len = 1;
                               break;
                        case 1:byt_len = 2;
                               break;
                        case 2:byt_len = 3;
                               break;
                        case 3:byt_len = 4;
                               break;
                        case 4:
                        case 5:
                        case 6:
                        case 7:
                               byt_len = 8;
                               break;
                        default:
                               break;
                    }

                    if (is_pk || type == INSERT_UNPACK) {
                        i     = 0; 
                        j     = 1; 
                        d_int = 0; 
                        while(i < byt_len){
                            d_int += UCHAR(ptr +i) * j; 
                            j = j * 256; 
                            i++;
                        }

                        snprintf(pk_val[k], COL_LEN, "%s = %ld ", 
                                schema->column[k].col_name, d_int);
                    }

                    ptr+=byt_len;
                    break;
                case MYSQL_TYPE_ENUM: //enum
                    byt_len = schema->column[k].length > 255?2:1;

                    if (is_pk || type == INSERT_UNPACK) {
                        if (byt_len == 2)
                            d_int = uint2korr(ptr);
                        else
                            d_int = UCHAR(ptr);

                        snprintf(pk_val[k], COL_LEN, "%s = %ld ", 
                                schema->column[k].col_name, d_int);
                    }

                    ptr+=byt_len;
                    break;
                case MYSQL_TYPE_GEOMETRY: //spatial
                    break;
                case MYSQL_TYPE_STRING:            //char or varchar
                case MYSQL_TYPE_VAR_STRING:
                case MYSQL_TYPE_BLOB:                // text
                    byt_len = get_byte_by_length(schema->column[k].length);
                    switch(byt_len) {
                        case 1: length = UCHAR(ptr);
                                break;
                        case 2: length = uint2korr(ptr);
                                break;
                        case 3: length = uint3korr(ptr);
                                break;
                        case 4: length = uint4korr(ptr);
                                break;
                    }

                    ptr+=byt_len;

                    if (is_pk || type == INSERT_UNPACK) {
                        bzero(str,RECORDLEN);
                        memcpy(str, ptr, length);
                        str[length] = '\0';

                        snprintf(pk_val[k], COL_LEN, "%s = \'%s\' ", 
                                schema->column[k].col_name, str);
                    }

                    ptr+=length;
                    break;
                case MYSQL_TYPE_TIME:   //time,3 bytes
                    if (is_pk || type == INSERT_UNPACK) { 
                        d_int = uint3korr(ptr);

                        snprintf(pk_val[k], COL_LEN, "%s = %ld ", 
                                schema->column[k].col_name, d_int);
                    }
                    ptr+=3;
                    break;
                case MYSQL_TYPE_TIMESTAMP:    //4bytes,timestamp
                    if (is_pk || type == INSERT_UNPACK) {
                        pk = uint4korr(ptr);
                        snprintf(pk_val[k], COL_LEN, "%s = FROM_UNIXTIME(%lld) ", 
                                schema->column[k].col_name, pk);
                   }
                    ptr+=4;
                    break;
                case MYSQL_TYPE_DATE:         //3bytes
                    if (is_pk || type == INSERT_UNPACK) {
                        d_int = uint3korr(ptr);
                        snprintf(pk_val[k], COL_LEN, "%s = %04ld%02ld%02ld ", 
                                schema->column[k].col_name,
                                ((d_int&((1<<24)-512))>>9), 
                                ((d_int&480)>>5), ((d_int&31)));
                   }

                    ptr+=3;
                    break;
                case MYSQL_TYPE_YEAR:
                    if (is_pk || type == INSERT_UNPACK) {
                        d_int = UCHAR(ptr);
                        d_int = year_2000_handling(d_int);
                        snprintf(pk_val[k], COL_LEN, "%s = %ld ", 
                                schema->column[k].col_name, d_int);
                    }

                    ptr+=1;
                    break;
                case MYSQL_TYPE_DATETIME:   /// 8bytes,datetime

                    if (is_pk || type == INSERT_UNPACK) {
                        day_val = uint8korr(ptr); //such as gmtmodified
                        snprintf(pk_val[k], COL_LEN, "%s = %lld ", 
                                schema->column[k].col_name, day_val);
                        
                   }

                    ptr+=8;
                    break;
                default:
                    break;
            }

            if (found == 1 && type == SELECT_UNPACK)
              count++;

        } 
        k++;
    }

    if ((type == SELECT_UNPACK) ){
      if (count != 0) {
        /*format sql to execute*/
        int i=0;
        for (;i<schema->uk_num;i++) {
         char sql[RECORDLEN];
         snprintf(sql, RECORDLEN, "select * from %s.%s where ", 
                               schema->db, tbName);//this query may be changed to delete from, so here don't change; 
         int j=0;
         for (;j<schema->uk_ct[i];j++) {
           if (j == 0)
             snprintf(sql+strlen(sql), RECORDLEN-strlen(sql), " %s ", pk_val[schema->uk_bit[i][j]-1]);
           else
             snprintf(sql+strlen(sql), RECORDLEN-strlen(sql), " and %s ", pk_val[schema->uk_bit[i][j]-1]);
         }
         
         snprintf(record[i], RECORDLEN, "%s", sql);
         bzero(sql, RECORDLEN);
        }
      }else { 
        if (setting.debug) {
          DEBUG("can't find key,tb:%s", tbName);
        }
        *error = 1;
      }
     } 

    return ptr;
}

/*re-init glob_schema and glob_reader instance */
int reinit_reader(void) 
{
    int i = 0;
    for (i=0; i<setting.n_schema; i++) {
       free(glob_schema[i]);
       glob_schema[i] = NULL;
    }
    
    mysql_close(&(glob_reader->mysql));

    free(glob_reader);
    glob_reader = NULL;

    init_reader();
    update_slave_status();

    glob_reader->relayNum = glob_reader->slaveNum;
    glob_reader->relayPos = glob_reader->slavePos;

    return 0;
}


/*read length of bytes into buf from fd */
int read_file(int fd, char* buf, int length)
{
    ssize_t ret = 0;
    int waitCount = 0;
    int readCount = 0;
    
    if (length>RECORDLEN) {
        DEBUG("WARNING:ignore too big event:    \
                len=%d, limit=%d\n", 
                length, RECORDLEN);
        sleep(10);
        return -1;
    }

repeat:
    ret = read(fd, buf, length);
    if (ret < 0) {
        DEBUG("error happen while read event:%s..", strerror(errno));
        return -1;
    } else if (ret == 0) {
        /* in case of purged relay log */
        waitCount++;
        if (setting.debug &&  (waitCount%100) == 0) {
            ulonglong curPos = lseek(fd, 0, SEEK_CUR);
            DEBUG("wait for updating,  relay:%d, pos:%lld", 
                    glob_reader->relayNum, curPos);
        }

         /* to remove 2000 */
        if (waitCount>10) {
            update_slave_status();
            if (glob_reader->relayNum == glob_reader->slaveNum)
                waitCount = 0;
            else {
                DEBUG("this relay log may be deleted. relay:%d\n"
                      "force to reset reader thread...\n",
                      glob_reader->relayNum);
                return -1;
            }    
        }
        os_thread_sleep(SLEEP_TIME*5);
        goto repeat;
    } else if (ret < length) {
        /* in case of corrupted events */
        readCount++;
        if (setting.debug && (readCount%20) == 0) {
            DEBUG("too small read, so lseek back and re-read it!!");
        }

        if (readCount > 400) {
            DEBUG("file may corrupted, need to reinit.");
            return -1;
        } 

        lseek(fd, 0-ret, SEEK_CUR);
        os_thread_sleep(SLEEP_TIME*5);
        goto repeat;
    }
   
   return 0;
} 


/* read head and body of event from relay log file.
 * return -1 if failed, else return 0
 */
int read_event_from_relay(int fd, char *buf)
{
    ssize_t ret = 0;
   /*read event header*/ 
    ret = read_file(fd, buf, LOG_EVENT_MINIMAL_HEADER_LEN);
    
    if (ret == -1)
        goto error;
        
    if (buf[EVENT_TYPE_OFFSET] == ROTATE_EVENT)
        return 0;

    /*read rest of this event*/
    uint data_len = uint4korr(buf + EVENT_LEN_OFFSET);
    ret = read_file(fd, buf+LOG_EVENT_MINIMAL_HEADER_LEN,
            data_len-LOG_EVENT_MINIMAL_HEADER_LEN);

error:
    return ret;
}


static inline long long get_relaylog_num(const char *str)
{
   int i = 0;
   if (str == NULL || str[0] == '\0') {
      DEBUG("get relaylog file name error. just exist and check!");
      exit(1);
   }

   i = strlen(str)-1;
   while (i>=0 && str[i] != '.')
       i--;

   if (i<0) {
       DEBUG("get relaylog file name error. just exist and check!");
       exit(1);
   }

   i++;
   return atoll(str+i);   
}


int update_slave_status(void)
{
    int ret   = 0;
    int error = 0;
    int has_rp1 = 0;
    int has_rp2 = 0;
    int has_rp3 = 0;
    int has_rp4 = 0;
    
    char sql[200];
    snprintf(sql, 200, "show slave status");
    MYSQL* mysql = &(glob_reader->mysql);

 repeat:

    // my_query would not returned until connection is OK
    ret = my_query(mysql, sql);
    
	if (ret != 0) {
        /* should not happen here */
       if (!has_rp1) { 
           DEBUG("error happen:%s", mysql_error(mysql));
           has_rp1 = 1;
       }
	   os_thread_sleep(SLEEP_TIME*200);
       goto repeat;
	}

    MYSQL_RES *res = NULL; 
    if (!(res = mysql_store_result(mysql))) {
        /* some thing unexpected happen here */
        if (!has_rp2) {
            DEBUG("'show slave status' error:%s, retrying ...\n",
                    mysql_error(mysql));
            has_rp2 = 1;
        }
        os_thread_sleep(SLEEP_TIME*200);
        goto repeat;
    }

    MYSQL_ROW row;
    if (!(row = mysql_fetch_row(res))) {
        /* it's not slave any more, might master-slave fail-over or 
         * something else. slave is an independed instance now.
         */
        if (!has_rp3) {
            DEBUG("slave has stopped, relay-fetch retrying ...");
            has_rp3 = 1;
        }
        mysql_free_result(res);
        os_thread_sleep(SLEEP_TIME*1200); // sleep 60s
        goto repeat;
    }
    
    
    glob_reader->slaveNum = get_relaylog_num(row[7]); 
    glob_reader->slavePos = atoll(row[8]);
    if (row[32] != NULL) {
        /* sql_thread is running */
        glob_reader->seconds_behind = atoi(row[32]);        
        pthread_mutex_lock(&rf_monitor_mutex);
        rf_seconds_behind = glob_reader->seconds_behind;
        pthread_mutex_unlock(&rf_monitor_mutex);
    }
    else {
        if (!has_rp4) {
            DEBUG("slave may stopped, so just wait untile slave start...");
            has_rp4 = 1;
        }

        if (setting.ha_error) {
            if (row[36] != NULL && row[37] != NULL && atoll(row[36]) != 0) {
                pthread_mutex_lock(&slave_error.mutex);

                bzero(slave_error.err_str, sizeof(slave_error.err_str));
                slave_error.bin_pos       = atoll(row[21]); 
                slave_error.trx_start_pos = glob_reader->slavePos;
                slave_error.file_num      = glob_reader->slaveNum;
                slave_error.err = atoll(row[36]);

                snprintf(slave_error.err_str, sizeof(slave_error.err_str), "%s", row[37]);
                slave_error.fix_in_process = 1;
                pthread_cond_signal(&(slave_error.cond));
                pthread_mutex_unlock(&(slave_error.mutex));
                
                pthread_mutex_lock(&(slave_error.wait_mutex));
                if (slave_error.fix_in_process)
                    pthread_cond_wait(&(slave_error.wait_cond), &(slave_error.wait_mutex));

                pthread_mutex_unlock(&(slave_error.wait_mutex));    
            }
        }
        os_thread_sleep(SLEEP_TIME*200);
        mysql_free_result(res);
        goto repeat;
    }

    mysql_free_result(res); 

    return error;
}


int test_if_wait_slave(int fd, int flags) 
{
    ulonglong curPos = lseek(fd, 0, SEEK_CUR);
    
    if (flags == READER_CHECK_FLAG) {
        int event_len = (curPos - glob_reader->relayPos)/setting.count;
        if (0 == event_len) {
            DEBUG("some exception happen, just exit to check !");
            exit(1);
        }
        setting.count = setting.limit/event_len; 
        if (setting.count <=5000) {
            setting.count = 5000;
        } else if (setting.count >=10000) {
            setting.count = 10000;
        }
    }
    
    int count = 0;
    long long distance = 0;
    rf_fake_read = 0;

repeat:
    update_slave_status(); 
    
    if (glob_reader->seconds_behind < setting.seconds_behind)
        rf_fake_read = 1;

    if (glob_reader->relayNum == glob_reader->slaveNum) {
        if (flags == FILE_END_FLAG) {
            /* 1) rotate event, wait sql_thread switched to next relay log */
            if (setting.debug) {
                count++;
                if ((count%5) == 0) {
                    if (setting.debug) {
                        DEBUG("at rotate postion now sleep %d to wait sql thread", 
                                SLEEP_TIME*4);
                    }
                    count=0;
                }
            }
            os_thread_sleep(SLEEP_TIME*4);
            goto repeat;    
        }

        distance = curPos - glob_reader->slavePos;
        /*2) reader is behind sql thread, set rf_fake_read = 1 and return */
        if (distance<=0) {
            rf_fake_read = 1;
            return 0;

        } else if (distance > setting.limit) {
            /*3)reader is ahead of sql thread more than limit, wait until not
             * exceed limited distance */
            if (setting.debug) {
                count++;
                if ((count%20)==0) {
                    if (setting.debug) {
                        DEBUG("distance:%lld, limit:%lld, now sleep %d ms", 
                                distance, setting.limit, SLEEP_TIME);
                    }
                    count=0;
                }
            }
            os_thread_sleep(SLEEP_TIME*4);
            goto repeat;

        } else if (flags == DDL_WAIT_FLAG) {
            /*4) if reader find a ddl sql, then reader wait until sql threads over
             * reader position, that is to ignore the DDL effects to schema */
            if (setting.debug) {
                count++;
                if ((count%4)==0) {
                    if (setting.debug) {
                        DEBUG("find a ddl sql, so wait,distance:%llu, now sleep %d ms", 
                                distance, SLEEP_TIME);
                    }
                    count=0;
                }
            }
            os_thread_sleep(SLEEP_TIME);
            goto repeat;
        } 
        else
            return 0;

    } else {        
        /*sql thread exceed reader one more file*/
        rf_fake_read = 1;
        if (flags == FILE_END_FLAG) {
            glob_reader->relayNum = glob_reader->slaveNum;
            glob_reader->relayPos = glob_reader->def_pos;

            return 1;
        }

        return 0;
    }
}


char* skip_space(char *src)
{
    int i = 0;
    int len = strlen(src);

    while (i<len && (src[i] == ' ' 
                  || src[i] == '\n'
                  || src[i] == '\t'))
        i++;

    return src+i;
}


/*drop a record from glob_schema array*/
int drop_tb_from_schema(int slot, int db_slot)
{
    if (slot != db_slot) 
        glob_schema[db_slot]->group--;
    else if (slot != glob_schema[db_slot]->group) 
        // more than one element in current group
        glob_schema[slot+1]->group = glob_schema[slot]->group;      

    free(glob_schema[slot]);
    glob_schema[slot] = NULL;

    int begin = slot;
    int end = setting.n_schema-1;

    for(;begin<end;begin++) {
        glob_schema[begin] = glob_schema[begin+1];
        glob_schema[begin]->group--;
    }

    setting.n_schema--;

    return 0;
}


int load_or_drop_schema(char* db, char* tb, int flag)
{
   int slot = find_tb_index (db, tb);
   int db_slot = 0;
   int ret = 0;

   if (slot != -1) {  // found slot in glob_schema list
       switch(flag) {
           case OP_DROP:
               glob_schema[slot]->tb_num--;
               if (glob_schema[slot]->tb_num == 0) {
                   db_slot = find_db_index(db);
                   drop_tb_from_schema(slot, db_slot);    
               }
               break;
           case OP_CREATE: 
               ret = get_columns_info(glob_schema[slot], tb);
               glob_schema[slot]->tb_num++;
               break;
           case OP_ALTER:
               glob_schema[slot]->n_modify++;
               if (glob_schema[slot]->n_modify >= 
                      glob_schema[slot]->tb_num) {
                   ret = get_columns_info(glob_schema[slot], tb);
                   glob_schema[slot]->n_modify = 0;
               }

               break;
           default:
               break;
       }

       return 0;
   }
   
   if (flag == OP_DROP || flag ==  OP_ALTER) {
       DEBUG("table:%s, database:%s not exist in schema list to drop/alter", tb, db); 
       return 0;
   }

   /*create a new entry in glob_schema*/
   int beg = 0;
   int end = 0;
   int new_slot = 0;
   db_slot = find_db_index(db);

   /*db is not in glob_schema, so create new (db,tb) slot*/
   if (db_slot == -1)
       record_table_info(db,tb);
   else 
   { /*find a slot for this tb*/
       schema_info_st *tmp = NULL;
       tmp = (schema_info_st*)malloc(sizeof(schema_info_st));
       bzero(tmp, sizeof(schema_info_st));
       snprintf(tmp->db, 200, "%s", db);
       ret = get_columns_info(tmp, tb);
       
       if (ret == -1) { 
           //can't find this table,may drop immediately after create
           free(tmp);
           return -1;
       }

       beg = glob_schema[db_slot]->group+1;
       end = setting.n_schema-1;
       for (;end>=beg;end--) {
          glob_schema[end+1] = glob_schema[end];
          glob_schema[end+1]->group++;
       }
       
       new_slot = beg;

       glob_schema[new_slot] = tmp;

       char *p1;
       char *p2;
       p1 = p2 = NULL;
       p1 = tb;
       while ((p1 = strstr(p1, "_")) != NULL) {
           p2 = p1;
           p1++;
       }

       if (setting.is_part && p2 != NULL 
               && p2[1] != '\0' && isdigit(p2[1]))
           snprintf(glob_schema[new_slot]->tb, p2-tb+2, "%s", tb);
       else
           snprintf(glob_schema[new_slot]->tb, 200, "%s", tb);

       glob_schema[new_slot]->group      = new_slot;
       glob_schema[new_slot]->n_modify   = 0;
       glob_schema[new_slot]->tb_num     = 1;

       glob_schema[db_slot]->group++;
       setting.n_schema++;
       return 0;
  }
  
  return 0;
}


int drop_db_from_schemas(char *db)
{
    int db_slot = find_db_index(db);
    
    if (db_slot == -1)
        return 0;

    int begin   = db_slot;
    int end     = glob_schema[db_slot]->group+1;
    int num     = end-begin;
    int n_free  = 0; 

    for (; end<setting.n_schema; end++) {
        if (++n_free <= num)
            free(glob_schema[begin]);
        glob_schema[begin] = glob_schema[end];
        glob_schema[begin]->group-=num;
        begin++;
    }
    
    setting.n_schema-=num;

    return 0;
}


int test_if_ddl(char *sql, char *db, int fd)
{
    int ret = 0;
    int i = 0;
    char *ptr = NULL;
    char tb[200];
    bzero(tb,200);

    ptr=skip_space(sql);

    for (i=0;i<DDLNUM;i++) {
        if (strncasecmp(ptr,str_ddl[i], 
                    strlen(str_ddl[i])) == 0) {

            int len = strlen(str_ddl[i]);
            ptr+=len;
            ptr = skip_space(ptr);
            if (strncasecmp(ptr, "table", 5) == 0) {
                ptr+=5;
                ptr=skip_space(ptr);
                
                if ((strncasecmp(ptr, "if", 2)==0) && 
                        (strncasecmp(skip_space(ptr+2), "exists", 6)==0)) 
                    ptr = skip_space(skip_space(ptr+2)+6); /*ignore if exists*/
                
                int j = 0;
                while(isalnum(*ptr) || *ptr == '_' || *ptr == '$') {
                    tb[j] = *ptr;
                    j++;
                    ptr++;
                }

            } else {
                if (i == OP_DROP && 
                        strncasecmp(ptr, "database", 8) == 0) {
                    if (setting.debug) {
                        DEBUG("DDL:drop db, DB:%s,SQL:%s", db, sql); 
                    }
                    drop_db_from_schemas(db);
                }

                return 0;    
            }


            test_if_wait_slave(fd, DDL_WAIT_FLAG);
            
            if (setting.debug) {
               DEBUG("DDL:%s, DB:%s, TB:%s, SQL:%s", str_ddl[i], db, tb, sql); 
            }
            ret = load_or_drop_schema(db, tb, i);
            return ret;
        } 
    }

    return 0;
}


/* repalce \t, \n */
static inline int format_sql(char *sql)
{  
   int i = 0;
   while (sql[i] != '\0') {
      if (sql[i] == '\t' || 
                sql[i] == '\n')
          sql[i] = ' ';
      
      i++;
   }    
   
   return 0;
}


/* only QUERY_EVENT is processed here, with record passed out 
 * return -1 for error, else return slot of table */
int handle_query_event(int fd, char *sql, char *db, char *record)
{
   char *ptr = NULL;
   char *tmp = NULL;
   int is_dml = 0;
   char tb[500];
   bzero(tb, 500);
   
   format_sql(sql);
   ptr = skip_space(sql);
   
   if ((strncasecmp(ptr, "begin", 5)==0)  ||
           (strncasecmp(ptr, "insert", 6) == 0))    //insert query event
       return -1;
   else if (strncasecmp(ptr, "update", 6) == 0) {    //update query event
      is_dml = 1;
      ptr+=6;
   } 
   else if (strncasecmp(ptr, "delete", 6) == 0) {
      is_dml = 1;
      ptr+=6;
      ptr = strcasestr(ptr, "from ")+5;
   } 
   
   if (is_dml) {
       ptr = skip_space(ptr);
       int j = 0;
       while(isalnum(*ptr) || *ptr == '_' || *ptr == '$') { //get table name
           tb[j] = *ptr;
           j++;
           ptr++;
       }

       ptr = strcasestr(ptr, "where ");
       while (ptr != NULL && 
               (tmp = strcasestr(ptr+6, "where ")) != NULL)
           ptr = tmp;

       if ((!rf_fake_read) && ptr != NULL) {
           ptr+=6;
           snprintf(record, RECORDLEN, "select * from %s.%s where %s", db, tb, ptr);
           return find_tb_index(db, tb);
       }
       
       return -1;
   }
   else
     test_if_ddl(sql, db, fd);

   return -1;
}


static int reset_rec(char **rec)
{
  int i=0;
  for (;i<MAX_UK;i++)
    rec[i][0]='\0';
  
  return 0;
}

void *do_reader_thread(void *arg)
{
    int slot     = -1;
    int slot_num = 0;
    int count    = 0;
    ulonglong file_pos  = 0;
    int error= 0;
    int ret  = 0;
    int i    = 0;
    int when = 0;
    int cols = 0;
    const char* ptr = NULL;
    unsigned int data_len = 0;
    int flag = 0;
    int ignore   = 0;
    char dbName[200];
    char tbName[200];
    char sql[10000];
    char* record[MAX_UK];
    for (i=0; i< MAX_UK; i++) {
      record[i] = (char *)malloc(RECORDLEN); 
    }
    
    i=0;
    char *buf = (char *)malloc(RECORDLEN*sizeof(char));
    bzero(buf, RECORDLEN);

    setting.limit = setting.limit * 1024 * 1024;
    
    char relay_path[500];
    snprintf(relay_path, 500, "%s.%06d", glob_reader->path, 
                                        glob_reader->relayNum);
    int relayfd = 0;
    relayfd = open(relay_path, O_RDONLY, 0);

    if (relayfd == -1) {
        perror("error while open file:");
        exit(1);
    }

    lseek(relayfd, glob_reader->relayPos, SEEK_SET);
    DEBUG("begin read relay log:%s", relay_path);
    
    unsigned long long tm_tid =0;
    unsigned long long row_tid=0;
    while (setting.running) {
        if (count >= setting.count) { 
            test_if_wait_slave(relayfd,READER_CHECK_FLAG);
            count = 0; 
            file_pos = lseek(relayfd, 0, SEEK_CUR);
            glob_reader->relayPos = file_pos;
        }
    
        error = 0;
        bzero(buf, RECORDLEN);
        ret = read_event_from_relay(relayfd, buf);
        count++;
        file_pos = lseek(relayfd, 0, SEEK_CUR);
        pthread_mutex_lock(&rf_monitor_mutex);
        rf_event_read++ ;
        pthread_mutex_unlock(&rf_monitor_mutex);

        /* fatal error, reset reader and glob_schema */
        if (ret == -1) {
           count = 0;
           DEBUG("WARNING: need reset schema!");
           reinit_reader();
           rf_fake_read = 1;
           close(relayfd);
           snprintf(relay_path, 500, "%s.%06d", glob_reader->path, 
                                                  glob_reader->relayNum);
           DEBUG("reset and start new file:%s, Pos:%lld", relay_path,
                                            glob_reader->relayPos);

           relayfd = open(relay_path, O_RDONLY|O_NONBLOCK, 0);
           if (relayfd == -1) {
                 DEBUG("error happen while open file,errror num:%d", errno);
                 exit(1);
           }

           lseek(relayfd, glob_reader->relayPos, SEEK_SET);
            
           continue;
        }

        if (buf[EVENT_TYPE_OFFSET] == ROTATE_EVENT) {
            DEBUG("Now in Rotate Postion,File:%06d, Pos:%lld", glob_reader->relayNum, file_pos);
            int n_err = 0;
            count = 0;
            while (1) {
                test_if_wait_slave(relayfd, FILE_END_FLAG);
                close(relayfd);
                snprintf(relay_path, 500, "%s.%06d", glob_reader->path, 
                                                  glob_reader->relayNum);
                DEBUG("start new file:%s, Pos:%lld", relay_path,
                                                   glob_reader->relayPos);

                relayfd = open(relay_path, O_RDONLY|O_NONBLOCK, 0);
                if (relayfd == -1) {
                    DEBUG("error happen while open file,errror num:%d", errno);
                    n_err++;
                    if (n_err == 10) {
                        DEBUG("can't open file try 10 times, just exit.") ;
                        exit(1);
                    }
                    continue;
                }
                lseek(relayfd, glob_reader->relayPos, SEEK_SET);
                break;
            }
        }

        if (buf[EVENT_TYPE_OFFSET] == QUERY_EVENT) {
            bzero(dbName, 200);
            //char *start = buf+19+13+uint2korr(buf+19+11);
            char *start = buf+32+uint2korr(buf+30);
            snprintf(dbName, 200, "%s", start);
            if (dbName[0] == '\0') {
                if (setting.debug) {
                   DEBUG("Error:GET db From Query_Event error, just ignore it");
                }
                continue;
            } else
            if (strcasecmp(dbName, "test") == 0 || strcasecmp(dbName, "mysql") == 0)
                continue;

            bzero(sql, sizeof(sql));
            snprintf(sql, sizeof(sql), "%s", start+strlen(dbName)+1);

            if (sql == '\0') {
                if (setting.debug) {
                   DEBUG("Error:GET SQL From Query_Event Error, just ignore it");
                }
                continue;
            }
            
            reset_rec(record);
            ret = handle_query_event(relayfd, sql, dbName, record[0]);
            if (ret>=0) {   // get a record, statement
                slot_num = slot_num%setting.n_worker;
                if (setting.debug) {
                   DEBUG("QUERY_EVENT, file Pos:%lld, query:%s", file_pos, record[0]);
                }

                put_data_to_buffer(slot_num, record, 0);
                slot_num++;
            }
        }
        
        if (buf[EVENT_TYPE_OFFSET] == TABLE_MAP_EVENT) {
            
            tm_tid = uint6korr(buf+19);
            snprintf(dbName, sizeof(dbName), "%s", buf+28 );
            i = 29;
            while (buf[i] != '\0')
                i++;

            while (buf[i] == '\0')
                i++;

            snprintf(tbName, sizeof(tbName), "%s", buf+i+1);
            ignore = 0;
            
            slot = find_tb_index(dbName, tbName);
            if (slot == -1) { 
                if (strcasecmp(dbName, "mysql") != 0 
                          && strcasecmp(dbName, "test") != 0) {
                    DEBUG("can't find table:%s, database:%s", tbName, dbName);
                }

                ignore = 1;
                continue;
            }
 
            if (rf_fake_read)
                continue;
           
            if (glob_schema[slot]->n_modify > 0) {
                if (setting.debug) {
                    DEBUG("here use table rule, so wait until all table altered");
                }
                ignore = 1;
                continue;
            }
           
            ignore = 0;
        }
        
        if (buf[EVENT_TYPE_OFFSET] == WRITE_ROWS_EVENT) {
            row_tid = uint6korr(buf+19);
            if (ignore || rf_fake_read || (tm_tid != row_tid))
                continue;
            
            cols = (buf[27]+7)/8;
            when =  uint4korr(buf);
            ptr = buf + 9;
            data_len = UCHAR(ptr) + (UCHAR(ptr+1)<<8) + (UCHAR(ptr+2)<<16)  + (UCHAR(ptr+3)<<24);
                
            ptr = buf+28 +cols;
                
            while ((ptr-buf)<data_len) {
                reset_rec(record);
                ptr = unpack_record(ptr, glob_schema[slot], tbName, record, SELECT_UNPACK, &error);
                if (!error) {
                    slot_num = slot_num%setting.n_worker;
                    put_data_to_buffer(slot_num, record, 0);
                    slot_num++;
                } else {
                    if (setting.debug) {
                        DEBUG("no pk or pk_col, %s,%s", dbName, tbName);
                    }
                }
            }
        }

        if (buf[EVENT_TYPE_OFFSET] == UPDATE_ROWS_EVENT) {
            row_tid = uint6korr(buf+19);
            if (ignore || rf_fake_read || (tm_tid != row_tid))
                continue;

            cols =(buf[27] +7)/8;
            ptr = buf + 9;
            data_len = UCHAR(ptr) + (UCHAR(ptr+1)<<8) + (UCHAR(ptr+2)<<16)  + (UCHAR(ptr+3)<<24);
                
            ptr = buf + 28 + cols * 2;
            when =  uint4korr(buf);
            flag = 0;

            while((ptr-buf)<data_len) {
                reset_rec(record);
                ptr = unpack_record(ptr, glob_schema[slot], tbName, record, SELECT_UNPACK, &error);
                if (flag == 0 && !error) {
                    slot_num = slot_num%setting.n_worker;
                    put_data_to_buffer(slot_num, record, 0);
                    slot_num++;
                } else {
                    if (flag == 0 && setting.debug) {
                        DEBUG("no pk or pk_col,%s,%s", dbName, tbName);
                    }
                }
                
                flag = (flag+1)%2;
            }
        }

        if (buf[EVENT_TYPE_OFFSET] == DELETE_ROWS_EVENT) {
            row_tid = uint6korr(buf+19);
            if (ignore || rf_fake_read || (tm_tid != row_tid))
                continue;
            
            cols = (buf[27]+7)/8;
            when =  uint4korr(buf);
            ptr = buf + 9;
            data_len = UCHAR(ptr) + (UCHAR(ptr+1)<<8) + (UCHAR(ptr+2)<<16)  + (UCHAR(ptr+3)<<24);
                
            ptr = buf+28 +cols;
            while ((ptr-buf)<data_len) {
                reset_rec(record);
                ptr = unpack_record(ptr, glob_schema[slot], tbName, record, SELECT_UNPACK, &error);
                if (!error) {
                    slot_num = slot_num%setting.n_worker;
                    put_data_to_buffer(slot_num, record, 0);
                    slot_num++;
                } else {
                    if (setting.debug) {
                        DEBUG("no pk or pk_col, %s,%s", dbName, tbName);
                    }
                }
            }
        }
    }
    for (i=0;i<MAX_UK;i++)
       free(record[i]);
    free(buf);
    pthread_exit(0);
}


static int daemon_rf() 
{
    umask(0);
    int fd, len;

    switch (fork()) {
        case -1:
            DEBUG("fork() failed:%s", strerror(errno));
            return -1;
        case 0: /* child */
            break;
        default: /* parent */
            exit(0);
    }
    setsid();

    printf("\n\n");
    DEBUG("###################start###################");
    
    if ((fd = open(setting.pidfile, O_RDWR|O_CREAT|O_SYNC|O_TRUNC, 0644)) == -1) {
        DEBUG("open pidfile error:%s,path:%s", strerror(errno), setting.pidfile);
        exit(0);
    }

    char buf[INT64_LEN + 1];
    len = snprintf(buf, INT64_LEN + 1, "%lu", (uint64_t)getpid());
    
    if (write(fd, buf, len) != len) {
        DEBUG("write error:%s", strerror(errno));
        return -1;
    }

    if (close(fd)) {
        DEBUG("close error:%s", strerror(errno));
        return -1;
    }
    
    return 0;
}


int init_setting() 
{
    setting.daemon         = 0;
    setting.debug          = 0;
    setting.running        = 1;
    setting.user           = NULL;
    setting.password       = NULL;
    setting.cnf_file       = NULL; /* not used yet */
    setting.port           = 0;
    setting.limit          = 1;
    setting.count          = 5000;
    setting.n_schema       = 0;
    setting.n_worker       = 5;
    setting.socket         = "/u01/mysql/run/mysql.sock";
    setting.seconds_behind = 1;
    setting.is_part        = 1;
    setting.monitor        = 0;
    setting.logdir         = NULL;
    setting.ha_error       = 0;
    setting.dry_run        = 0;
    return 0;
}


int init_variables() 
{   
    int ret = 0;
    ret = (pthread_mutex_init(&rf_logfile_mutex,NULL) || pthread_mutex_init(&rf_monitor_mutex,NULL));
    if (ret != 0) {
        DEBUG("Error happen, errno:%d", errno);
        exit(1);

    }

    int i = 0;
    for (i=0; i<setting.n_worker; i++){
        mysql_init(&(glob_worker[i].mysql));
        connect_mysql(&(glob_worker[i].mysql), NULL);
        glob_worker[i].index = i;
    }

    sems_full= (sem_t*)malloc(sizeof(sem_t)* setting.n_worker);
    sems_empty= (sem_t*)malloc(sizeof(sem_t)*setting.n_worker);

    node = (node_t*) malloc(sizeof(node_t) * setting.n_worker);
    buffer = (char***)malloc(sizeof(char**) * setting.n_worker);
    for (i=0; i<setting.n_worker; i++) {
        sem_init(&sems_full[i], 0, SLOT_ITEMS);
        sem_init(&sems_empty[i],0, 0);
        node[i].head=node[i].tail=node[i].loops=0;

        buffer[i] = (char**) malloc(sizeof(char*) * SLOT_ITEMS);
        int j = 0;
        for (j=0; j<SLOT_ITEMS; j++)
            buffer[i][j] = (char*) malloc(ITEM_LEN);
    }

    return 0;
}


void init_slave_error()
{
     /*init slave_errror*/

    slave_error.trx_err_pos  = 0;
    slave_error.trx_start_pos = 0;
    slave_error.err           = 0;
    bzero(slave_cmd, 1000);
    if (setting.socket != NULL)
        snprintf(slave_cmd, 1000, "mysql -uroot -S %s test -e 'select now();"
                                       "show slave status\\G' >> %s/rf.log", 
                                                  setting.socket, setting.logdir);
    else
        snprintf(slave_cmd, 1000, "mysql -uroot -h127.0.0.1 -p %d test -e 'select now();"
                                "show slave status\\G' >> %s/rf.log",
                                                  setting.port, setting.logdir);
   
    pthread_cond_init(&(slave_error.cond), NULL);

    pthread_mutex_init(&(slave_error.mutex), NULL);
}


void usage() {
    printf("%s\n%s\n%s\n%s\n%s\n\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n",
    "relayfetch for mysql version: 0.1.0",
    "usage:",
    " ./relayfetch -t -u[user] -p[password] -S[socket] -n[thread num] ",
    " # only support row, int primary key, and need sudo privileges #",
    "",
    "arguments:",
    "   -d               debug                                                      ",
    "   -D               daemon                                                     ",
    "   -k               kill relayfetch when run in daemon                         ",
    "   -p<String>       password                                                   ",
    "   -u<String>       user name                                                  ",
    "   -P<NUM>          port of mysqld                                             ",
    "   -s<NUM>          limit to awoid too fast warm up, default 1M                ",
    "   -c<NUM>          evety NUM querys to check distance, default 2000           ",
    "   -S<String>       socket path, defaults value :/u01/mysql/run/mysql.sock     ",
    "   -n<NUM>          worker threads num, less than 100                          ",
    "   -a<NUM>          work if secends_behind_master is more than NUM, default 1s ",
    "   -t               if use this option, means not using table rules            ",
    "   -m               print monitor info                                         ",
    "   -l               log/tmpfile dir                                            ",
    "   -e               create a thread to handle slave error                      ",
    "   -r               dry run slave err handle                                   ",
    "   -h               help");
}


int get_options(int argc, char *argv[]) {

    int c;
    while ((c = getopt (argc, argv, "remdDhkta:S:c:n:s:P:u:p:l:")) != -1) {
        switch (c)
        {
            case 'd':
                setting.debug  = 1;
                break;
            case 'r':
                setting.dry_run = 1;
                break;
            case 'l':
                setting.logdir = optarg;
                break;
            case 'e':
                setting.ha_error = 1;
                break;
            case 'a':
                setting.seconds_behind = atoi(optarg);
                if (setting.seconds_behind == 0)
                    setting.seconds_behind = 1;
                break;
            case 'm':
                setting.monitor = 1;
                break;
            case 'n':
                setting.n_worker  = atoi(optarg);
                if (setting.n_worker >= 100 || 
                        setting.n_worker <= 0) {
                    usage();
                    abort();
                }
                break;
            case 'D':
                setting.daemon = 1;
                break;
            case 's':
                setting.limit = atoi(optarg);
                if (setting.limit == 0) {
                    usage();
                    abort();
                }
                break;
            case 'c':
                setting.count = atoi(optarg);
                if (setting.count == 0) {
                    usage();
                    abort();
                }
                break;
            case 'k':
                glob_kill=1;
                break;
            case 'u':
                setting.user = optarg;
                break;
            case 'p':
                setting.password = optarg;
                break;
            case 'S':
                setting.socket = optarg;
                break;
            case 'P':
                setting.port = atoi(optarg);
                break;
            case 't':
                setting.is_part = 0;
                break;
            case 'h':
                usage();
                exit(-1);
            case '?':
                if (optopt == 'f')
                    fprintf (stderr, "Option -%c requires an argument.\n", optopt);
                else if (isprint (optopt))
                    fprintf (stderr, "Unknown option `-%c'.\n", optopt);
                else
                    fprintf (stderr,
                            "Unknown option character `\\x%x'.\n",
                            optopt);
                usage();
                abort();
            default:
                //rf_kill();
                abort ();
        }
    }

    return 0;
}


int init_file()
{   
    if (setting.logdir == NULL)
        setting.logdir = "/tmp";
    else{
        if (setting.logdir[strlen(setting.logdir)-1] == '/')
            setting.logdir[strlen(setting.logdir)-1] = '\0';
    } 

    char cnf_com[PATH_MAX];

    bzero(cnf_com,PATH_MAX);
    snprintf(cnf_com, MAX_STRING_LEN, "%s/rf.pid", setting.logdir);
    setting.pidfile = strdup(cnf_com);

    bzero(cnf_com,PATH_MAX);
    snprintf(cnf_com, MAX_STRING_LEN, "%s/rf.log", setting.logdir);
    setting.logfile = strdup(cnf_com);

    if ((fp_stdout=freopen(setting.logfile,"a",stdout))==NULL) {
        fprintf(stderr,"error freopen %s:%s\n",setting.logfile,  strerror(errno));
        exit(1);
    }

    return 0;
}


int main(int argc, char * argv[]) 
{
   /*ignore SIG_HUP*/
    struct sigaction sa;
    sa.sa_handler=SIG_IGN;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags=0;
    if (sigaction(SIGHUP,&sa,NULL)<0)
        printf("sig error:SIGHUP\n");
       
    if (mysql_library_init(0, NULL, NULL)) {
        fprintf(stderr, "could not initialize MySQL library\n");
        exit(1);
    }

    /*init global setting structure*/
    init_setting();
    get_options(argc, argv);
    
    /*init log file and pid file*/ 
    if (init_file())
        return -1;

    if(glob_kill) {
        rf_kill();
        abort();
    }
    
    init_slave_error();
   
    pthread_t ha_error_tid;
    init_reader();
    
    /*worker structure to control worker threads*/
    glob_worker = (worker *)calloc(setting.n_worker, 
            sizeof(worker)); 
  
    if (init_variables())
        return -1;
    FILE *fp;
    char path[PATH_MAX];
    snprintf(path, PATH_MAX, "%s", "rf.pid");
    if ((fp=fopen(path,"r")) != NULL) {
        char buf[70]="/proc/";
        char pid[60];
        if (fgets(pid,60,fp)==NULL) {
            DEBUG("error while fgets");
            return -1;
        }    

        strcat(buf,pid);

        DIR *dp;
        if ((dp=opendir(buf)) != NULL) {
            printf("rf is already running\n");
            closedir(dp);
            exit(0);
        } else {
            printf("rf is not running, start it\n");
        }
    }
    
    if (setting.daemon)
        daemon_rf();
    
    int i = 0;

    /*create monitor thread*/
    pthread_t monitor_tid;
    if (setting.monitor)
        create_thread(&monitor_tid, do_monitor_thread, NULL);

/*create read thread*/
    DEBUG("create reader thread.");
    create_thread(&(glob_reader->tid), do_reader_thread, NULL);

    /*create worker thread*/
    for (i = 0; i != setting.n_worker; i++) {
        DEBUG("create worker thread:%d", i);
        create_thread(&(glob_worker[i].tid), do_worker_thread, glob_worker+i);
    }

    for (i = 0; i != setting.n_worker; i++) {
        pthread_join(glob_worker[i].tid, NULL);
    }

    pthread_join(glob_reader->tid, NULL);
    if (setting.monitor)
        pthread_join(monitor_tid, NULL);
    
    if (setting.ha_error)
        pthread_join(ha_error_tid, NULL);

    rf_clean(0);

    return 0;
}

