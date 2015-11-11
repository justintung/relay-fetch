/* Copyright 2012 Taobao.com

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


#ifndef _RELAY_FETCH_H_
#define _RELAY_FETCH_H_

#include <ctype.h>
#include <signal.h>
#include <arpa/inet.h>  //htonl & ntohl
#include <dirent.h>
#include <errno.h>
#include <my_global.h>
#include <mysql_com.h>
#include <my_dir.h>
#include <my_sys.h>
#include <mysql.h>
#include <time.h>
#include <unistd.h>
#include <m_string.h>
#include <sys/types.h>
#include <errmsg.h>
#include <string.h>
#include <sys/mman.h>
#undef sem_wait
#undef sem_post
#undef assert
#include <assert.h>

enum Log_event_type
{
  /*
    Every time you update this enum (when you add a type), you have to
    fix Format_description_log_event::Format_description_log_event().
  */
  UNKNOWN_EVENT= 0,
  START_EVENT_V3= 1,
  QUERY_EVENT= 2,
  STOP_EVENT= 3,
  ROTATE_EVENT= 4,
  INTVAR_EVENT= 5,
  LOAD_EVENT= 6,
  SLAVE_EVENT= 7,
  CREATE_FILE_EVENT= 8,
  APPEND_BLOCK_EVENT= 9,
  EXEC_LOAD_EVENT= 10,
  DELETE_FILE_EVENT= 11,
  NEW_LOAD_EVENT= 12,
  RAND_EVENT= 13,
  USER_VAR_EVENT= 14,
  FORMAT_DESCRIPTION_EVENT= 15,
  XID_EVENT= 16,
  BEGIN_LOAD_QUERY_EVENT= 17,
  EXECUTE_LOAD_QUERY_EVENT= 18,

  TABLE_MAP_EVENT = 19,

  PRE_GA_WRITE_ROWS_EVENT = 20,
  PRE_GA_UPDATE_ROWS_EVENT = 21,
  PRE_GA_DELETE_ROWS_EVENT = 22,
  WRITE_ROWS_EVENT = 23,
  UPDATE_ROWS_EVENT = 24,
  DELETE_ROWS_EVENT = 25,
  INCIDENT_EVENT= 26,

  /*
    Add new events here - right above this comment!
    Existing events (except ENUM_END_EVENT) should never change their numbers
  */

  ENUM_END_EVENT /* end marker */
};

#define LOG_EVENT_MINIMAL_HEADER_LEN 19
#define EVENT_TYPE_OFFSET    4
#define SERVER_ID_OFFSET     5
#define EVENT_LEN_OFFSET     9
#define LOG_POS_OFFSET       13
#define FLAGS_OFFSET         17


FILE *fp_stdout;
const char *LOCAL_BASE_DIR;

#define DEBUG my_printf
#define BIN_LOG_HEADER_SIZE    4
#define PROBE_HEADER_LEN    (EVENT_LEN_OFFSET+4)
#define POSFILE_POS_SIZE 4
#define LOG_POS_INIT     4
#define POSFILE_STR_LEN_SIZE 4
#define INT64_LEN   (sizeof("-9223372036854775808") - 1)
#define THREAD_STACK_SIZE (1024 * 1024 * 8)  /* 8M */
#define MAX_STRING_LEN      1024
#define EVENT_INTERVAL      1000

#define MAX_PK      10

//#include <my_decimal.h>

typedef enum
{TRUNCATE=0, HALF_EVEN, HALF_UP, CEILING, FLOOR}
  decimal_round_mode;
typedef int32 decimal_digit_t;

typedef struct st_decimal_t {
  int    intg, frac, len;
  my_bool sign;
  decimal_digit_t *buf;
} decimal_t;


typedef decimal_digit_t dec1;
typedef longlong      dec2;

#define DIG_PER_DEC1 9
#define DIG_MASK     100000000
#define DIG_BASE     1000000000
#define DIG_MAX      (DIG_BASE-1)
#define DIG_BASE2    ((dec2)DIG_BASE * (dec2)DIG_BASE)
#define ROUND_UP(X)  (((X)+DIG_PER_DEC1-1)/DIG_PER_DEC1)
static const dec1 powers10[DIG_PER_DEC1+1]={
      1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};
static const int dig2bytes[DIG_PER_DEC1+1]={0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
static const dec1 frac_max[DIG_PER_DEC1-1]={
    900000000, 990000000, 999000000,
    999900000, 999990000, 999999000,
    999999900, 999999990 };


#define E_DEC_OK                0
#define E_DEC_TRUNCATED         1
#define E_DEC_OVERFLOW          2
#define E_DEC_DIV_ZERO          4
#define E_DEC_BAD_NUM           8
#define E_DEC_OOM              16

#define E_DEC_ERROR            31
#define E_DEC_FATAL_ERROR      30

#define DECIMAL_BUFF_LENGTH 9
#define DECIMAL_MAX_POSSIBLE_PRECISION (DECIMAL_BUFF_LENGTH * 9)
#define DECIMAL_MAX_PRECISION (DECIMAL_MAX_POSSIBLE_PRECISION - 8*2)

#define FIX_INTG_FRAC_ERROR(len, intg1, frac1, error)                   \
        do                                                              \
        {                                                               \
          if (unlikely(intg1+frac1 > (len)))                            \
          {                                                             \
            if (unlikely(intg1 > (len)))                                \
            {                                                           \
              intg1=(len);                                              \
              frac1=0;                                                  \
              error=E_DEC_OVERFLOW;                                     \
            }                                                           \
            else                                                        \
            {                                                           \
              frac1=(len)-intg1;                                        \
              error=E_DEC_TRUNCATED;                                    \
            }                                                           \
          }                                                             \
          else                                                          \
            error=E_DEC_OK;                                             \
        } while(0)

#define mi_sint1korr(A) ((int8)(*A))
#define mi_uint1korr(A) ((uint8)(*A))

#define mi_sint2korr(A) ((int16) (((int16) (((uchar*) (A))[1])) +\
                                  ((int16) ((int16) ((char*) (A))[0]) << 8)))
#define mi_sint3korr(A) ((int32) (((((uchar*) (A))[0]) & 128) ? \
                                  (((uint32) 255L << 24) | \
                                   (((uint32) ((uchar*) (A))[0]) << 16) |\
                                   (((uint32) ((uchar*) (A))[1]) << 8) | \
                                   ((uint32) ((uchar*) (A))[2])) : \
                                  (((uint32) ((uchar*) (A))[0]) << 16) |\
                                  (((uint32) ((uchar*) (A))[1]) << 8) | \
                                  ((uint32) ((uchar*) (A))[2])))
#define mi_sint4korr(A) ((int32) (((int32) (((uchar*) (A))[3])) +\
                                  ((int32) (((uchar*) (A))[2]) << 8) +\
                                  ((int32) (((uchar*) (A))[1]) << 16) +\
                                  ((int32) ((int16) ((char*) (A))[0]) << 24)))
#define mi_sint8korr(A) ((longlong) mi_uint8korr(A))

#define decimal_make_zero(dec)        do {                \
                                        (dec)->buf[0]=0;    \
                                        (dec)->intg=1;      \
                                        (dec)->frac=0;      \
                                        (dec)->sign=0;      \
                                      } while(0)



/////////////////////////////////
//
typedef struct {
    int head;
    int tail;
    int loops;
} node_t;

#include<semaphore.h>

node_t *node;
sem_t *sems_full;
sem_t *sems_empty;
char ***buffer;

#define SLOT_ITEMS          64 
#define ITEM_LEN            (1024 * 4)
#define N_STATS_ITV         1000

typedef struct {
    pthread_t tid;
    int index;
    MYSQL mysql;
} worker;

typedef struct {
    const char *pidfile;
    const char *logfile;
    const char *autoinc_id;
    const char *cnf_file;
    char *logdir;
    char *socket;
    char *user;
    char *password;
    int mode;
    int port;
    int daemon;
    int n_schema;
    int n_worker;
    int debug;
    int running;
    int count;
    int seconds_behind;
    int is_part;
    int monitor;
    int ha_error;
    int dry_run;
    ulonglong limit;
}setting_st;

typedef struct mysql_info_st {
    const char* host;
    const char* user;
    const char *database;
    const char *table;
    const char* password;
    const char* name;
    short port;
    int server_id;
} mysql_info_st;


#define MAX_UK 64
typedef struct col_type{
    int  id;
    char col_name[200];
    int  col_type;
    long long  length;
    int  is_pk_or_uk;
    int  is_sign;
    int decimals;
} col_type;

typedef struct schema_info_st {
    char db[200];
    char tb[200];
    int group;
    int cols;
    int n_modify;
    int tb_num;
    int has_pk;
    col_type column[1024];
    int uk_num;
    int  uk_bit[MAX_UK][100];//store unique index
    int  uk_ct[MAX_UK]; //columns of each index
} schema_info_st;

typedef struct reader {
    MYSQL mysql;
    char path[500];
    char host[200];
    int  relayNum;
    int  slaveNum;
    int version;
    int def_pos;
    ulonglong relayPos;
    ulonglong slavePos;
    int seconds_behind;
    long long distance;
    pthread_t tid;
} reader_st;

#define DDLNUM 3
static char str_ddl[DDLNUM][10] = {"alter", "drop", "create" };
enum {OP_ALTER, OP_DROP, OP_CREATE};

enum {
READER_CHECK_FLAG = 0,
FILE_END_FLAG = 1,
DDL_WAIT_FLAG = 2
};

#endif
