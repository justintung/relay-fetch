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


#include "seh.h"

#define RECORDLEN 62000
#define SLEEP_TIME 50000
#define UCHAR(ptr) ((*(ptr)+256)%256)

static char slave_cmd[1000];
static setting_st   setting;
static int kill_seh  =  0;
static int sleep_time = 60;

typedef struct slave_info_st{
   pthread_t tid;
   int port;
   char err_str[2000];
   char path[500];
   char slave_cmd[1000];;
   long long int bin_pos;
   long long int trx_start_pos;
   long long int trx_err_pos;
   int           err;   
   int           file_num;
   int           fix_in_process;
   int           is_success;
} slave_info_st;

#define MAX_SLAVE_NUM 100
slave_info_st slave_queue[MAX_SLAVE_NUM];

enum {
SELECT_UNPACK,
INSERT_UNPACK
};

pthread_mutex_t   rf_logfile_mutex;
int get_columns_info(slave_info_st *si, schema_info_st* schema, char * tb);
int update_slave_status(slave_info_st *si);
int read_event_from_relay(int fd, char *buf);
int init_slave();
const char*  unpack_record(const char *ptr, schema_info_st * schema,  
                      char *tbName, char * record, int type, int* error);

static inline void get_curr_time(char *result) {
    time_t now=time(0);
    struct tm tnow;
    bzero(&tnow,sizeof(struct tm));
    localtime_r(&now, &tnow);
    sprintf(result,"%d-%d %d:%d:%d", 
            tnow.tm_mon+1,
            tnow.tm_mday,
            tnow.tm_hour,
            tnow.tm_min,
            tnow.tm_sec);
}

static char time_buf[256];                             
#define my_printf(fmt, ...)                         \
    pthread_mutex_lock(&rf_logfile_mutex);          \
    fflush(fp_stdout);                              \
    bzero(time_buf,256);                            \
    get_curr_time(time_buf);                        \
    printf("%s: ",time_buf);                        \
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


int connect_mysql(MYSQL* mysql, char *db, int port) 
{
    int reported = 0;
  
    while (1) {
        if (!(mysql_real_connect(mysql, 
                        "127.0.0.1",
                        setting.user,
                        setting.password,
                        db, 
                        port, 
                        NULL, 
                        0))) {
            if (!reported) {
                DEBUG("Couldn't connect to server Error:%s,"
                        "errno:%d, port: %d,sleeping 60s for reconnecting\n", 
                        mysql_error(mysql),
                        mysql_errno(mysql),
                        port);
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
int seh_kill()
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


/*record a table information in glob_schema array*/
schema_info_st* record_table_info(slave_info_st *info, char *db, char *tb)
{
    
    schema_info_st * si = NULL;

    si = (schema_info_st*)malloc(sizeof(schema_info_st));
    bzero(si, sizeof(schema_info_st));

    snprintf(si->tb, 200, "%s", tb);
    snprintf(si->db, 200, "%s", db);
  
    int ret = get_columns_info(info, si, tb);
    if (ret == -1) {
        DEBUG("read table schema info error....");
        free(si);
        return NULL;  //just ignore this table
    }
    
    return si;
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


int insert_or_delete_for_error(char *sql, int type , int port)
{

    if (type == 1062) {
        assert(strncmp(sql, "select", 6) == 0);

        sql[0]='d';sql[1]='e';sql[2]='l';sql[3]='e';
        sql[4]='t';sql[5]='e';sql[6]=' ';sql[7]=' ';
    }


    DEBUG("excute query:%s", sql);
    
    MYSQL* my = NULL;
    my = mysql_init(NULL);

    connect_mysql(my, NULL, port);
    int ret = 0;
    
    if (setting.dry_run)
        goto end;

    char query[200];
    snprintf(query, 200, "set session sql_log_bin = 'OFF'");
    if ((ret=mysql_query(my, query)) != 0) {
        DEBUG("query error: %s,"  
                "127.0.0.1,errno:%d",   
                mysql_error(my), mysql_errno(my));

        goto end;
    }
    /*excure  sql*/
    if ((ret=mysql_query(my, sql)) != 0) {
        DEBUG("query error: %s,"  
                "127.0.0.1,errno:%d",   
                mysql_error(my), mysql_errno(my));

    } else
    {
       DEBUG("excute success!");    
    }
   
   /* just ignore error of this sql...*/
    ret = 0;

end:
    mysql_close(my);
    return ret;
}


int start_slave(int port)
{
    MYSQL* my = NULL;
    my = mysql_init(NULL);

    connect_mysql(my, NULL, port);
    int ret = 0;
    
    DEBUG("excute:start slave");
    if (setting.dry_run)
        goto end;

    /*start slave*/
    if ((ret=mysql_query(my, "start slave")) != 0) {
        DEBUG("query error: %s,"  
                "127.0.0.1,errno:%d",   
                mysql_error(my), mysql_errno(my));

        DEBUG("sql=START SLAVE");
        goto end;
    } else
    {
       DEBUG("excute success!"); 
    }
    /*check slave status*/
    if ((ret=mysql_query(my, "show slave status")) != 0) {
        DEBUG("query error: %s,"  
                "127.0.0.1,errno:%d",   
                mysql_error(my), mysql_errno(my));

        DEBUG("sql=show slave status");
        goto end;
    }
    
    MYSQL_RES *res = NULL; 
    res = mysql_store_result(my);
    MYSQL_ROW row;
    if (!(row = mysql_fetch_row(res))) {
        mysql_free_result(res);
        ret = -1;
        goto end;
    }
    
    if (atoll(row[36]) == 0)
        ret = 0;
    else 
        ret = -1;
    
    mysql_free_result(res);

end:
    mysql_close(my);
    return ret;
}


int handle_1032_error(slave_info_st * si)
{
    char buf[RECORDLEN];

    const char *ptr = strstr(si->err_str, "end_log_pos ");
    if (ptr == NULL) {
        DEBUG("can't find end_log_pos in err string");
        return -1;
    }

    ptr+=strlen("end_log_pos ");
    
    if (atoll(ptr) == 0) {
       DEBUG("Error happen while trying to get master postion"); 
       return -1;  
    }
    
    si->trx_err_pos = atoll(ptr)-si->bin_pos+si->trx_start_pos; 
  
    long long int end_pos = si->trx_err_pos;
    long long int cur_pos = si->trx_start_pos;
    
    char path[500];
    snprintf(path, 500, "%s.%06d", si->path, 
                                        si->file_num);
    DEBUG("slave error happen:%s\n"
          "error file:%s\n" 
          "error trx begin pos:%lld\n"
          "error event end pos:%lld",
          si->err_str,
          path,
          cur_pos,
          end_pos
         );
    
    int relayfd = 0;
    relayfd = open(path, O_RDONLY, 0);

    if (relayfd == -1) {
       DEBUG("error happen while open file:%s:%s..", path, strerror(errno));
       return -1;
    }

    lseek(relayfd, cur_pos, SEEK_SET);
    bzero(buf, sizeof(buf));
    int ret = -1;
    int i=0;
    char db_name[500];
    char tb_name[500];
    schema_info_st* schema_info = NULL;
    long long tm_tid  = 0;
    long long row_tid = 0;
    int count=0; 
    while(cur_pos<end_pos) {
        ret = read_event_from_relay(relayfd, buf);
        count++;
        if (ret == -1) {
            DEBUG("read event error, cur pos:%lld", cur_pos);
            goto end;
        }
        
        if (count == 1) {  //check if need to adjust end_pos
          long long dis= lseek(relayfd, 0, SEEK_CUR) - cur_pos;
          long long adj=  uint4korr(buf+13);
          long long relend = atoll(ptr)-(adj-dis)+si->trx_start_pos;
          if (relend != end_pos) {
           DEBUG("relend:%lld, gotend:%lld, reset end_pos", relend, end_pos);
           end_pos= relend;
          }
        }

        if (buf[4] == TABLE_MAP_EVENT) {
            tm_tid = uint6korr(buf+19);
            snprintf(db_name, sizeof(db_name), "%s", buf+28 );
            i = 29;
            while (buf[i] != '\0')
                i++;

            while (buf[i] == '\0')
                i++;

            snprintf(tb_name, sizeof(tb_name), "%s", buf+i+1);
        }

        cur_pos = lseek(relayfd, 0, SEEK_CUR);
    }

   if (cur_pos != end_pos) {
      DEBUG("some errr may happen while try to read event where error happen,"
           "cur_pos:%lld", cur_pos) ;
      ret = -1;
      goto end;
   }
   
   if (uint4korr(buf+13) != (atoll(ptr))) {
      DEBUG("Get Pos Error...\n");
      ret = -1;
      goto end;
   }

    schema_info = record_table_info(si, db_name, tb_name);
    if (schema_info == NULL || schema_info->has_pk == 0) {
        DEBUG("no schema info or has no pk column..");
        goto end;
    }
    int cols=0;
    ptr = NULL;
    int data_len;
    unsigned int when;
    int flag;
    int error=0;

    char query[RECORDLEN];
    if (buf[4] == UPDATE_ROWS_EVENT) {
        row_tid = uint6korr(buf+19);
        if (row_tid != tm_tid) {
            DEBUG("can't match table id");
            goto end;
        }
        cols =(buf[27] +7)/8;
        ptr = buf + 9;
        data_len = UCHAR(ptr) + (UCHAR(ptr+1)<<8) + (UCHAR(ptr+2)<<16)  + (UCHAR(ptr+3)<<24);

        ptr = buf + 28 + cols * 2;
        when =  uint4korr(buf);
        flag = 0;

        while((ptr-buf)<data_len) {
            bzero(query, RECORDLEN);
            ptr = unpack_record(ptr, schema_info, tb_name, query, INSERT_UNPACK, &error);
            
            if (flag == 0)
                ret = insert_or_delete_for_error(query, 1032, si->port);
            
            if (ret != 0) {
                DEBUG("call insert_or_delete_for_error failed");
                goto end;
            }

            flag = (flag+1)%2;
        
        }
        
        ret = start_slave(si->port);

    } else if (buf[4] == DELETE_ROWS_EVENT) {
        row_tid = uint6korr(buf+19);
        if (row_tid != tm_tid) {
            DEBUG("can't match table id ");
            goto end;
        }

        cols = (buf[27]+7)/8;
        when =  uint4korr(buf);
        ptr = buf + 9;
        data_len = UCHAR(ptr) + (UCHAR(ptr+1)<<8) + (UCHAR(ptr+2)<<16)  + (UCHAR(ptr+3)<<24);

        ptr = buf+28 +cols;
        while ((ptr-buf)<data_len) {
            bzero(query ,RECORDLEN);
            ptr = unpack_record(ptr, schema_info, tb_name, query, INSERT_UNPACK, &error);
            ret = insert_or_delete_for_error(query, 1032, si->port);

            if (ret != 0) {
                DEBUG("call insert_or_delete_for_error failed");
                goto end;
            }
        }

        ret = start_slave(si->port);

    } else {
        DEBUG("unmatch event type:%d", buf[4]);
        ret = -1;
    }

end:
   if (schema_info != NULL)
       free(schema_info);
   close(relayfd);
   
   if (ret != 0){
      DEBUG("failed to fix slave error...\n");
   }

   if (setting.dry_run) {
       DEBUG("---DRY-RUN MODE.");
   }

   return ret;
   
}


int handle_1062_error(slave_info_st *si)
{
    char buf[RECORDLEN];

    const char *ptr = strstr(si->err_str, "end_log_pos ");
    if (ptr == NULL)
        return -1;
    
    ptr+=strlen("end_log_pos ");
    int last_err = atoll(ptr)-si->bin_pos+si->trx_start_pos; 
    if (si->trx_err_pos == last_err)
        return -1;
    
    DEBUG("slave error happen:%s, \ntry to fix it.", si->err_str);

    si->trx_err_pos = last_err;    

    long long int end_pos = si->trx_err_pos;
    long long int cur_pos = si->trx_start_pos;
    
    char path[500];
    snprintf(path, 500, "%s.%06d", si->path, 
                                        si->file_num);
    int relayfd = 0;
    relayfd = open(path, O_RDONLY, 0);

    if (relayfd == -1) {
        perror("error while open file:");
        return -1;
    }

    lseek(relayfd, cur_pos, SEEK_SET);
    bzero(buf, sizeof(buf));
    int ret = -1;
    int i=0;
    char db_name[500];
    char tb_name[500];
    
    long long tm_tid  = 0;
    long long row_tid = 0; 
    while(cur_pos<end_pos) {
        ret = read_event_from_relay(relayfd, buf);
        if (ret == -1)
            goto end;
        
        if (buf[4] == TABLE_MAP_EVENT) {
            tm_tid = uint6korr(buf+19);
            snprintf(db_name, sizeof(db_name), "%s", buf+28 );
            i = 29;
            while (buf[i] != '\0')
                i++;

            while (buf[i] == '\0')
                i++;

            snprintf(tb_name, sizeof(tb_name), "%s", buf+i+1);
        }

        cur_pos = lseek(relayfd, 0, SEEK_CUR);
    }

    schema_info_st* schema_info = record_table_info(si, db_name, tb_name);
    if (schema_info == NULL || schema_info->has_pk == 0)
        goto end;
        
    int cols=0;
    ptr = NULL;
    int data_len;
    unsigned int when;
    int flag;
    int error=0;

    char query[RECORDLEN];
    if (buf[4] == UPDATE_ROWS_EVENT) {
        row_tid = uint6korr(buf+19);
        if (row_tid != tm_tid)
            goto end;

        cols =(buf[27] +7)/8;
        ptr = buf + 9;
        data_len = UCHAR(ptr) + (UCHAR(ptr+1)<<8) + (UCHAR(ptr+2)<<16)  + (UCHAR(ptr+3)<<24);

        ptr = buf + 28 + cols * 2;
        when =  uint4korr(buf);
        flag = 0;

        while((ptr-buf)<data_len) {
            bzero(query, RECORDLEN);
            ptr = unpack_record(ptr, schema_info, tb_name, query, SELECT_UNPACK, &error);
            
            if (flag == 1)
                ret = insert_or_delete_for_error(query, 1062, si->port);
            
            if (ret != 0)
                goto end;

            flag = (flag+1)%2;
        
        }
        
        ret = start_slave(si->port);

    } else if (buf[4] == WRITE_ROWS_EVENT) {
        row_tid = uint6korr(buf+19);
        if (row_tid != tm_tid)
            goto end;

        cols = (buf[27]+7)/8;
        when =  uint4korr(buf);
        ptr = buf + 9;
        data_len = UCHAR(ptr) + (UCHAR(ptr+1)<<8) + (UCHAR(ptr+2)<<16)  + (UCHAR(ptr+3)<<24);

        ptr = buf+28 +cols;
        while ((ptr-buf)<data_len) {
            bzero(query ,RECORDLEN);
            ptr = unpack_record(ptr, schema_info, tb_name, query, SELECT_UNPACK, &error);
            ret = insert_or_delete_for_error(query, 1062, si->port);

            if (ret != 0)
                goto end;
        }

        ret = start_slave(si->port);

    } else
        ret = -1;

end:
   if (schema_info != NULL)
       free(schema_info);

   close(relayfd);
   return ret;
   
}


int handle_slave_error()
{
    
    slave_info_st * si = NULL;
    
    int ret = 0;
    int i = 0;
    DEBUG("SEH STARTED!!!");
    while(setting.running) {
        DEBUG("---begin check");
        init_slave();
        if (setting.slave_num == 0) {
            DEBUG("no instance or file  exist....\n");
            DEBUG("---end check");   
            sleep(sleep_time);
            continue;
        }

        for (i =0; i< setting.slave_num; i++) {
            si = slave_queue+i; 
            assert(si != NULL);
            ret = update_slave_status(si);

            if (si->err == 0) {
                if (ret == -2) {
                   DEBUG("---Port:%d, read_only = OFF", si->port); 
                } else
                if (ret == -1) {
                   DEBUG("---Port:%d, check status failed", si->port);
                } else {
                   DEBUG("---Port:%d, Nomal!", si->port);
                }

                continue;
            } else {
                DEBUG("---Port:%d, Error happen:%d", si->port, si->err);
            } 

            switch(si->err) {
                case 1032:
                    ret = handle_1032_error(si);//key not found
                    break;
                case 1062:
                    ret = handle_1062_error(si);  //duplicate key
                    break;
                case 1213:   //deadlock happen, just start slave;
                    //  ret = start_slave(si->port);
                    break;
                case 1593:  //ER_SLAVE_FATAL_ERROR,just try to start slave
                    //  ret = start_slave(si->port);
                default:
                    break;
            }

            si->err = 0;
        }

        DEBUG("---end check");   
        sleep(sleep_time);
   } 
   
   return 0;
}


int get_columns_info(slave_info_st *si, schema_info_st* schema, char * tb)
{
    MYSQL* mysql;
    MYSQL* my_set;
    MYSQL* my_is;

    mysql  = mysql_init(NULL);
    my_set = mysql_init(NULL);
    my_is  = mysql_init(NULL);

    connect_mysql(mysql,  schema->db, si->port);
    connect_mysql(my_set, schema->db, si->port);
    connect_mysql(my_is,  schema->db, si->port );


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

    ret = mysql_query(mysql, sql);
    if (ret != 0) {
        mysql_close(mysql);
        mysql_close(my_set);
        mysql_close(my_is);
        return -1;
    }

    ret_set = mysql_query(my_set, sql_set);
    ret_is  = mysql_query(my_is, sql_is);
 
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
            schema->column[i].is_pk = 1;
            schema->has_pk = 1;
        }
        else
            schema->column[i].is_pk = 0;

        i++;
    }

    schema->cols = i;
    mysql_free_result(res);
    mysql_free_result(res_set);
    mysql_free_result(res_is);
    mysql_close(mysql);
    mysql_close(my_set);
    mysql_close(my_is);

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



const char*  unpack_record(const char *ptr, schema_info_st * schema,  
                           char *tbName, char * record, int type, int* error)
{
    if (schema->cols == 0) {
        /*some error must happen because schema->cols has been inited in function read_tb_info */
        DEBUG("error happen, and exit now......");
        exit(1);
        //get_columns_info(schema, tbName);
    }

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
    if (type == SELECT_UNPACK)
        snprintf(record, RECORDLEN, "select * from %s.%s where ", 
                               schema->db, tbName);//this query may be changed to delete from, so here don't change; 
    else if (type == INSERT_UNPACK)
        snprintf(record, RECORDLEN, "insert into %s.%s select ", schema->db, tbName);

    int k = 0;
    char str[RECORDLEN];
    long int d_int;
    float d_f;
    double d_d;
    int byt_len = 0;
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
    int hasFoundPk = 0;
    long long pk   = 0;
    int str_len    = 0;
#define TMPLEN 2000
    char tmp[TMPLEN];
    char tmpf[320];
    while (k < cols) {
        byt    = (k+8)/8 - 1;
        by_nu  = k%8; 
        is_null = ((UCHAR(row_begin + byt)) & (1 << by_nu));

        if (is_null == 0) {
            is_pk   = schema->column[k].is_pk;
            is_sign = schema->column[k].is_sign;
            found = is_pk;
            switch(schema->column[k].col_type){
                case MYSQL_TYPE_LONG:         //int
                    if (is_pk || type == INSERT_UNPACK) { 
                        if (is_sign)
                            pk = sint4korr(ptr);
                        else
                            pk = uint4korr(ptr);

                        bzero(tmp, TMPLEN);
                        snprintf(tmp, TMPLEN, "%s = %lld ", 
                                schema->column[k].col_name, pk);

                        if (type == INSERT_UNPACK) {
                            snprintf(record+strlen(record), RECORDLEN-strlen(record), "%lld,", pk);    
                        }

                    }

                    ptr+=4;
                    break;
                case MYSQL_TYPE_TINY:   //tinyint, 1byte
                    if (is_pk || type == INSERT_UNPACK) {
                        if (is_sign)
                            d_int = (char)ptr[0];
                        else
                            d_int = (unsigned char)ptr[0];

                        bzero(tmp, TMPLEN);
                        snprintf(tmp, TMPLEN, "%s = %ld ", 
                                schema->column[k].col_name, d_int);

                        if (type == INSERT_UNPACK) {
                            snprintf(record+strlen(record), RECORDLEN-strlen(record), "%ld,", d_int);    
                        }
                    }  

                    ptr++;
                    break;
                case MYSQL_TYPE_SHORT:  //smallint,2byte
                    if (is_pk || type == INSERT_UNPACK) {
                        if (is_sign)
                            d_int = (int32)sint2korr(ptr);
                        else
                            d_int = (int32)uint2korr(ptr);

                        bzero(tmp, TMPLEN);
                        snprintf(tmp, TMPLEN, "%s = %ld ", 
                                schema->column[k].col_name, d_int);

                        if (type == INSERT_UNPACK) {
                            snprintf(record+strlen(record), RECORDLEN-strlen(record), "%ld,", d_int);    
                        }
                    }

                    ptr+=2;
                    break;
                case MYSQL_TYPE_INT24: //MEDIUMINT
                    if (is_pk || type == INSERT_UNPACK) {
                        if (is_sign)
                            d_int = sint3korr(ptr);
                        else
                            d_int = uint3korr(ptr);

                        bzero(tmp, TMPLEN);
                        snprintf(tmp, TMPLEN, "%s = %ld ", 
                                schema->column[k].col_name, d_int);

                        if (type == INSERT_UNPACK) {
                            snprintf(record+strlen(record), RECORDLEN-strlen(record), "%ld,", d_int);    
                        }

                    }

                    ptr+=3;
                    break;
                case MYSQL_TYPE_LONGLONG: //bigint
                    if (is_pk || type == INSERT_UNPACK) {
                        bzero(tmp, TMPLEN);
                        if (is_sign) {
                            si = sint8korr(ptr);
                            snprintf(tmp, TMPLEN, "%s = %lld ", 
                                    schema->column[k].col_name, si);
                            if (type == INSERT_UNPACK) {
                                snprintf(record+strlen(record), RECORDLEN-strlen(record), "%lld,", si);    
                            }
                        }
                        else {
                            usi = uint8korr(ptr);
                            snprintf(tmp, TMPLEN, "%s = %lld ", 
                                    schema->column[k].col_name, usi);
                            if (type == INSERT_UNPACK) {
                                snprintf(record+strlen(record), RECORDLEN-strlen(record), "%lld,", usi);    
                            }
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

                        bzero(tmp, TMPLEN);
                        snprintf(tmp, TMPLEN, "%s = %s ", 
                                schema->column[k].col_name, dec_str);

                        if (type == INSERT_UNPACK) {
                            snprintf(record+strlen(record), RECORDLEN-strlen(record), "%s,", dec_str);    
                        }

                    }

                    prec_len = decimal_bin_size(prec, decs);
                    ptr+=prec_len;
                    break;
                case MYSQL_TYPE_FLOAT:   //float
                    if (is_pk || type == INSERT_UNPACK) {
                        float4get(d_f, ptr);
                        bzero(tmpf,sizeof(tmpf));
                        sprintf(tmpf, "%-20g", (double) d_f);
                        bzero(tmp, TMPLEN);
                        snprintf(tmp, TMPLEN, "%s like %s", 
                                schema->column[k].col_name, tmpf);
                        if (type == INSERT_UNPACK) {
                            snprintf(record+strlen(record), RECORDLEN-strlen(record), "%s,", tmpf);    
                        }
                    }

                    ptr+=4;
                    break;
                case MYSQL_TYPE_DOUBLE: //double
                    if (is_pk || type == INSERT_UNPACK) {
                        float8get(d_d, ptr);
                        bzero(tmpf, sizeof(tmpf));
                        sprintf(tmpf, "%-20g", d_d);
                        bzero(tmp, TMPLEN);
                        snprintf(tmp, TMPLEN, "%s = %s ", 
                                schema->column[k].col_name,tmpf); 
                        if (type == INSERT_UNPACK) {
                            snprintf(record+strlen(record), RECORDLEN-strlen(record), "%s,", tmpf);    
                        }

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
                        bzero(tmp, TMPLEN);
                        snprintf(tmp, TMPLEN, "%s = %ld ", 
                                schema->column[k].col_name, d_int);
                        if (type == INSERT_UNPACK) {
                            snprintf(record+strlen(record), RECORDLEN-strlen(record), "%ld,", d_int);    
                        }

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

                        bzero(tmp, TMPLEN);
                        snprintf(tmp, TMPLEN, "%s = %ld ", 
                                schema->column[k].col_name, d_int);
                        if (type == INSERT_UNPACK) {
                            snprintf(record+strlen(record), RECORDLEN-strlen(record), "%ld,", d_int);    
                        }

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

                        bzero(tmp, TMPLEN);
                        snprintf(tmp, TMPLEN, "%s = %ld ", 
                                schema->column[k].col_name, d_int);
                        if (type == INSERT_UNPACK) {
                            snprintf(record+strlen(record), RECORDLEN-strlen(record), "%ld,", d_int);    
                        }

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

                        bzero(tmp, TMPLEN);
                        snprintf(tmp, TMPLEN, "%s = \'%s\' ", 
                                schema->column[k].col_name, str);
                        if (type == INSERT_UNPACK) {
                            snprintf(record+strlen(record), RECORDLEN-strlen(record), "\'%s\',", str);    
                        }

                    }

                    ptr+=length;
                    break;
                case MYSQL_TYPE_TIME:   //time,3 bytes
                    if (is_pk || type == INSERT_UNPACK) { 
                        d_int = uint3korr(ptr);

                        bzero(tmp, TMPLEN);
                        snprintf(tmp, TMPLEN, "%s = %ld ", 
                                schema->column[k].col_name, d_int);
                        if (type == INSERT_UNPACK) {
                            snprintf(record+strlen(record), RECORDLEN-strlen(record), "%ld,", d_int);    
                        }

                    }
                    ptr+=3;
                    break;
                case MYSQL_TYPE_TIMESTAMP:    //4bytes,timestamp
                    if (is_pk || type == INSERT_UNPACK) {
                        pk = uint4korr(ptr);
                        bzero(tmp, TMPLEN);
                        snprintf(tmp, TMPLEN, "%s = FROM_UNIXTIME(%lld) ", 
                                schema->column[k].col_name, pk);
                        if (type == INSERT_UNPACK) {
                            snprintf(record+strlen(record), RECORDLEN-strlen(record), "FROM_UNIXTIME(%lld),", pk);    
                        }

                    }

                    ptr+=4;
                    break;
                case MYSQL_TYPE_DATE:         //3bytes
                    if (is_pk || type == INSERT_UNPACK) {
                        d_int = uint3korr(ptr);
                        bzero(tmp, TMPLEN);
                        snprintf(tmp, TMPLEN, "%s = %04ld%02ld%02ld ", 
                                schema->column[k].col_name,
                                ((d_int&((1<<24)-512))>>9), 
                                ((d_int&480)>>5), ((d_int&31)));
                        if (type == INSERT_UNPACK) {
                            snprintf(record+strlen(record), RECORDLEN-strlen(record), "%04ld%02ld%02ld,",
                                ((d_int&((1<<24)-512))>>9), 
                                ((d_int&480)>>5), ((d_int&31))
                            );    
                        }
                    }

                    ptr+=3;
                    break;
                case MYSQL_TYPE_YEAR:
                    if (is_pk || type == INSERT_UNPACK) {
                        d_int = UCHAR(ptr);
                        d_int = year_2000_handling(d_int);
                        bzero(tmp, TMPLEN);
                        snprintf(tmp, TMPLEN, "%s = %ld ", 
                                schema->column[k].col_name, d_int);
                        if (type == INSERT_UNPACK) {
                            snprintf(record+strlen(record), RECORDLEN-strlen(record), "%ld,", d_int);    
                        }

                    }

                    ptr+=1;
                    break;
                case MYSQL_TYPE_DATETIME:   /// 8bytes,datetime

                    if (is_pk || type == INSERT_UNPACK) {
                        day_val = uint8korr(ptr); //such as gmtmodified
                        bzero(tmp, TMPLEN);
                        snprintf(tmp, TMPLEN, "%s = %lld ", 
                                schema->column[k].col_name, day_val);
                        
                        if (type == INSERT_UNPACK) {
                            snprintf(record+strlen(record), RECORDLEN-strlen(record), "%lld,", day_val);    
                        }
                    }

                    ptr+=8;
                    break;
                default:
                    break;
            }

            if (found == 1 && type == SELECT_UNPACK) {
                if (hasFoundPk) 
                    snprintf(record+strlen(record), RECORDLEN-strlen(record), 
                            " and %s ", tmp);
                else {
                    snprintf(record+strlen(record), RECORDLEN-strlen(record), 
                            " %s ", tmp);
                    hasFoundPk = 1;
                }

                found = 0;
            }
        } else if (type == INSERT_UNPACK) 
            snprintf(record+strlen(record), RECORDLEN-strlen(record), "NULL,");    

        k++;
    }

    if ((type == SELECT_UNPACK) && (!hasFoundPk)){
        if (setting.debug) {
            DEBUG("can't find key,tb:%s", tbName);
        }
        *error = 1;
    }

    if (type == INSERT_UNPACK)
        record[strlen(record)-1] = '\0';

    return ptr;
}


/*read length of bytes into buf from fd */
int read_file(int fd, char* buf, int length)
{
    ssize_t ret = 0;
    
    if (length>RECORDLEN) {
        DEBUG("WARNING:ignore too big event:    \
                len=%d, limit=%d\n", 
                length, RECORDLEN);
        return -1;
    }

    ret = read(fd, buf, length);
    if (ret < length) {
        DEBUG("error happen while read event:%s..", strerror(errno));
        return -1;
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
    
    if (ret == -1) {
        goto error;
    }
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


int update_slave_status(slave_info_st* si)
{
    int ret   = 0;
    si->err = 0;
    char sql[200];
    MYSQL* mysql = mysql_init(NULL);
    connect_mysql(mysql, NULL, si->port);
    
    snprintf(sql, 200, "show variables like 'read_only'");
    ret = mysql_query(mysql, sql);
	if (ret != 0) {
        /* should not happen here */
       ret = -1;
       goto end;
    }

    MYSQL_RES *res = NULL; 
    if (!(res = mysql_store_result(mysql))) {
        ret = -1;
        goto end;
    }

    MYSQL_ROW row;
    if (!(row = mysql_fetch_row(res))) {
        mysql_free_result(res);
        ret = -1;
        goto end;
    }

    if (row[1] == NULL || strcasecmp(row[1], "OFF") == 0) {
        mysql_free_result(res);
        ret = -2;
        goto end;
    }

    mysql_free_result(res);

    snprintf(sql, 200, "show slave status");
    ret = mysql_query(mysql, sql);
	if (ret != 0) {
        /* should not happen here */
       ret = -1;
       goto end;
    }

    res = NULL; 
    if (!(res = mysql_store_result(mysql))) {
        ret = -1;
        goto end;
    }

    if (!(row = mysql_fetch_row(res))) {
        mysql_free_result(res);
        ret = -1;
        goto end;
    }
    
    if (row[32] == NULL) {
        if (row[36] != NULL && row[37] != NULL && atoll(row[36]) != 0) {
            bzero(si->err_str, sizeof(si->err_str));
            si->bin_pos       = atoll(row[21]); 
            si->trx_start_pos = atoll(row[8]);
            si->file_num      = get_relaylog_num(row[7]);
            si->err = atoll(row[36]);

            snprintf(si->err_str, sizeof(si->err_str), "%s", row[37]);
        }
    }
    
    ret = 0;
    mysql_free_result(res);

end:
    mysql_close(mysql);
    return ret;
}


int init_setting() 
{
    setting.debug          = 0;
    setting.user           = NULL;
    setting.password       = NULL;
    setting.cnf_file       = NULL; /* not used yet */
    setting.port           = 0;
    setting.logdir         = NULL;
    setting.ha_error       = 0;
    setting.dry_run        = 0;
    setting.slave_num      = 0;
    setting.conf           = NULL;
    setting.running        = 1;
    return 0;
}


int init_slave_slot(slave_info_st * info)
{
    /*we can be sure info->port != 0*/
    MYSQL* my = mysql_init(NULL);

    connect_mysql(my, NULL, info->port);
    
    char sql[500];
    bzero(sql, 500);
    snprintf(sql, 500, "show variables like 'relay_log'");
    
    int ret = mysql_query(my, sql);
    if (ret != 0) {
       DEBUG("error happen while try to get relay_log,so exit..");
       exit(1);
    }

    MYSQL_RES *res = mysql_store_result(my);
    MYSQL_ROW row;

    if ((row = mysql_fetch_row(res)) != NULL) { 
        snprintf(info->path, 500, "%s", row[1]);
        char *ptr  = NULL;
        char *ptr2 = NULL;
        ptr = strstr(info->path, ".");
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
 
    /*init command*/
    snprintf(slave_cmd, 1000, "mysql -uroot -h127.0.0.1 -P %d test -e ' "
                                "show slave status\\G' >> %s/seh.log",
                                                  info->port, setting.logdir);
   
    mysql_free_result(res);
    mysql_close(my);
    return 0;
}


/*init slave_info*/
int init_slave()
{    
    int i = 0;
    setting.slave_num = 0;
    
    if (setting.conf != NULL) {
        if (access(setting.conf, 0) != 0)
            return -1;

        FILE* fp = fopen(setting.conf, "r");

        if (fp == NULL)
            return -1;

        char buf[1000];
        bzero(buf, 1000);
        int slot = 0;
        long int port = 0;
        while (fgets(buf, 1000, fp) != NULL){ 
            i=0;

            if (buf[i] == '\n') {
                bzero(buf, 1000);
                continue;
            }

            while(i<999 && 
                    (buf[i] ==' ' || buf[i] == '\t'))
                i++;

            if (i == 999 || buf[i] == '#' || !isdigit(buf[i])) { 
                bzero(buf, 1000);
                continue;
            }

            port = atol(buf+i);

            if (port == 0) {
                DEBUG("i guess configure file were has error...so exit and check it");
                fclose(fp);
                return -1;
            }

            bzero(&(slave_queue[slot]), sizeof(slave_info_st));
            slave_queue[slot].port    = port;
            init_slave_slot(slave_queue+slot);

            slot++;
            setting.slave_num++;
            bzero(buf, 1000);
        }

        fclose(fp);
    } else if (setting.port != 0 || (setting.port = 3306)) {
        bzero(&(slave_queue[0]), sizeof(slave_info_st));
        slave_queue[0].port    = setting.port;
        init_slave_slot(slave_queue+0);
        setting.slave_num++;
    } else
        return -1;


    if (setting.slave_num == 0)
        return -1;
    return 0;
}

void usage() {
    printf("%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n",
    "relayfetch for mysql version: 0.1.0",
    "usage:",
    " ./seh -x [muti instance configure file] &",
    "or ./seh -P [port] ",
    "",
    "arguments:",
    "   -d               debug                                                      ",
    "   -p<String>       password                                                   ",
    "   -P<NUM>          port of mysqld                                             ",
    "   -l               log/tmpfile dir                                            ",
    "   -r               dry run slave err handle                                   ",
    "   -x<String>       multi instance configure file.                             ",
    "   -D               run daemon                                                 ",
    "   -u               user name                                                  ",
    "   -k               kill seh                                                   ",
    "   -t<NUM>          every NUM s  to check slave                                ",
    "   -h               help");
}


int get_options(int argc, char *argv[]) {

    int c;
    while ((c = getopt (argc, argv, "remdDhkt:a:S:c:n:s:P:u:p:l:x:")) != -1) {
        switch (c)
        {
            case 't':
                sleep_time = atoi(optarg);
                if (sleep_time == 0) {
                   usage();
                   abort();
                }
                break;
            case 'd':
                setting.debug  = 1;
                break;
            case 'k':
                kill_seh = 1;
                break;
            case 'x':
                setting.conf = optarg;
                break;
            case 'r':
                setting.dry_run = 1;
                break;
            case 'l':
                setting.logdir = optarg;
                break;
            case 'D':
                setting.daemon = 1;
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
        setting.logdir = "/var/log";
    else{
        if (setting.logdir[strlen(setting.logdir)-1] == '/')
            setting.logdir[strlen(setting.logdir)-1] = '\0';
    } 

    char cnf_com[PATH_MAX];

    bzero(cnf_com,PATH_MAX);
    snprintf(cnf_com, MAX_STRING_LEN, "%s/seh.pid", setting.logdir);
    setting.pidfile = strdup(cnf_com);

    bzero(cnf_com,PATH_MAX);
    snprintf(cnf_com, MAX_STRING_LEN, "%s/seh.log", setting.logdir);
    setting.logfile = strdup(cnf_com);
    
    if (kill_seh)
        seh_kill();

    if (access(setting.pidfile, F_OK) == 0) {
       printf("seh may be running now because %s exist\n please check or remove it...\n", setting.pidfile);
       exit(1);
    } 

    if ((fp_stdout=freopen(setting.logfile,"a",stdout))==NULL) {
        fprintf(stderr,"error freopen %s:%s\n",setting.logfile,  strerror(errno));
        exit(1);
    }

    return 0;
}



static int daemon_seh() 
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
    if (setting.user == NULL)
       setting.user = "root";

       /*init log file and pid file*/ 
    if (init_file())
        return -1;
    
    if (init_slave() == -1) {
       DEBUG("multi instance configure file were not set, so exit!");
       exit(1);
    }

    if (setting.daemon)
        daemon_seh();

    handle_slave_error();

    return 0;
}

