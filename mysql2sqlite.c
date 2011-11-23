#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <getopt.h>
#include <linux/limits.h>

#include "sqlite3.h"
#include "mysql.h"

struct pair{
    unsigned int length;
    char str[2046];
};

typedef struct pair tableName;
typedef struct pair tableStruct;

typedef struct mysqlTableStruct {
    struct pair field;
    struct pair type;
    struct pair collation;
    struct pair null;
    struct pair key;
    struct pair defaultValue;
    struct pair extra;
    struct pair privileges;
    struct pair comment;
}mysqlTableStruct;

typedef struct mysqlIndexStruct {
    struct pair table;
    struct pair non_unique;
    struct pair key_name;
    struct pair seq_in_index;
    struct pair column_name;
    struct pair collation;
    struct pair cardinality;
    struct pair sub_part;
    struct pair packed;
    struct pair null;
    struct pair index_type;
    struct pair comment;
}mysqlIndexStruct;

int getListTables(MYSQL *pMySQL,tableName tableName[])
{
    MYSQL_RES *pMysqlResult = NULL;
    MYSQL_ROW row;

    unsigned int num_row = 0;

    char sql[512] = { 0 };

    int ret = 0;

    if(pMySQL == NULL) {
        printf("function getTableStruct parameter error.\n");
        ret = -1;
        goto END;
    }

    sprintf(sql,"SHOW TABLE STATUS WHERE comment <> 'VIEW';\n");

    ret = mysql_real_query(pMySQL,sql,(unsigned int)strlen(sql));
    if(ret != 0) {
        printf("call mysql_real_query error.error = %s\n",mysql_error(pMySQL));
        ret = -4;
        goto END;
    }

    pMysqlResult = mysql_store_result(pMySQL);
    if(pMysqlResult == NULL) {
            printf("call mysql_store_result error.error = %s\n",mysql_error(pMySQL));
            ret = -4;
            goto END;
    }

    for(num_row = 0; (row = mysql_fetch_row(pMysqlResult)) != NULL; num_row++) {
       unsigned long *lengths;
       lengths = mysql_fetch_lengths(pMysqlResult);
       tableName[num_row].length = (int) lengths[0];
       sprintf(tableName[num_row].str,"%.*s", (int) lengths[0], row[0] ? row[0] : "NULL");
    }

    ret = num_row;

END:
    if(pMysqlResult != NULL) {
        mysql_free_result(pMysqlResult);
    }
    return ret;
}

int getTableStruct(MYSQL *pMySQL,tableName tableName,tableStruct *tableStructOne)
{
    MYSQL_RES *pMysqlResult = NULL;
    MYSQL_ROW row;

    char sql[512] = { 0 };
    unsigned int num_row = 0;
    unsigned int num_row_i = 0;
    unsigned int num_col = 0;
    unsigned int num_fields = 0;

    int flag = 0;

    mysqlTableStruct *mysqlTableStructFULL = NULL;

    int ret = 0;

    if(pMySQL == NULL || tableStructOne == NULL) {
        printf("function getTableStruct parameter error.\n");
        ret = -1;
        goto END;
    }
    memset(tableStructOne,0,sizeof(tableStruct));

    sprintf(sql,"SHOW FULL COLUMNS FROM %s;\n",tableName.str);

    ret = mysql_real_query(pMySQL,sql,(unsigned int)strlen(sql));
    if(ret != 0) {
        printf("call mysql_real_query error.error = %s\n",mysql_error(pMySQL));
        ret = -4;
        goto END;
    }

    pMysqlResult = mysql_store_result(pMySQL);
    if(pMysqlResult == NULL) {
            printf("call mysql_store_result error.error = %s\n",mysql_error(pMySQL));
            ret = -4;
            goto END;
    }

    num_row = mysql_num_rows(pMysqlResult);
    if(num_row <= 0) {
        printf("call mysql_num_rows error.error = %s\n",mysql_error(pMySQL));
        ret = -4;
        goto END;
    }

    mysqlTableStructFULL = malloc(sizeof(mysqlTableStruct) * num_row);
    if(mysqlTableStructFULL == NULL) {
            printf("call malloc error.error = %s\n",strerror(errno));
            ret = -5;
            goto END;
    }

    num_fields = mysql_num_fields(pMysqlResult);
    while ((row = mysql_fetch_row(pMysqlResult))) {
        num_col = 0;
        unsigned long *lengths;
        lengths = mysql_fetch_lengths(pMysqlResult);

        /* Field */
        mysqlTableStructFULL[num_row_i].field.length = lengths[num_col];
        sprintf(mysqlTableStructFULL[num_row_i].field.str,"%.*s", (int) lengths[num_col], row[num_col]);
        num_col++;

        /* Type */
        mysqlTableStructFULL[num_row_i].type.length = lengths[num_col];
        sprintf(mysqlTableStructFULL[num_row_i].type.str,"%.*s", (int) lengths[num_col], row[num_col]);
        num_col++;

        /* collation */
        mysqlTableStructFULL[num_row_i].collation.length = lengths[num_col];
        sprintf(mysqlTableStructFULL[num_row_i].collation.str,"%.*s", (int) lengths[num_col], row[num_col]?row[num_col]:"NULL");
        num_col++;

        /* Null */
        mysqlTableStructFULL[num_row_i].null.length = lengths[num_col];
        sprintf(mysqlTableStructFULL[num_row_i].null.str,"%.*s", (int) lengths[num_col], row[num_col]);
        num_col++;

        /* PRIMARY KEY */
        mysqlTableStructFULL[num_row_i].key.length = lengths[num_col];
        sprintf(mysqlTableStructFULL[num_row_i].key.str,"%.*s", (int) lengths[num_col], row[num_col]);
        num_col++;

        /* default value */
        mysqlTableStructFULL[num_row_i].defaultValue.length = lengths[num_col];
        sprintf(mysqlTableStructFULL[num_row_i].defaultValue.str,"%.*s", (int) lengths[num_col], row[num_col]?row[num_col]:"NULL");
        num_col++;

        /* extra */
        mysqlTableStructFULL[num_row_i].extra.length = lengths[num_col];
        sprintf(mysqlTableStructFULL[num_row_i].extra.str,"%.*s", (int) lengths[num_col], row[num_col]);
        num_col++;

        /* privileges */
        mysqlTableStructFULL[num_row_i].privileges.length = lengths[num_col];
        sprintf(mysqlTableStructFULL[num_row_i].privileges.str,"%.*s", (int) lengths[num_col], row[num_col]);
        num_col++;

        /* comment */
        mysqlTableStructFULL[num_row_i].comment.length = lengths[num_col];
        sprintf(mysqlTableStructFULL[num_row_i].comment.str,"%.*s", (int) lengths[num_col], row[num_col]);
        num_col++;

        num_row_i++;
    }
    
    mysql_free_result(pMysqlResult);
    pMysqlResult = NULL;

    sprintf(sql,"CREATE TABLE %s(",tableName.str);
    strcat(tableStructOne->str,sql);

    for(num_row_i = 0; num_row_i < num_row; num_row_i++) {
        char *tmp = NULL;
        strcat(tableStructOne->str, mysqlTableStructFULL[num_row_i].field.str);
        strcat(tableStructOne->str, " ");

        tmp = strstr(mysqlTableStructFULL[num_row_i].type.str, "unsigned");
        if(tmp != NULL) {
            memset(tmp,0,strlen(tmp));
        }
        strcat(tableStructOne->str, mysqlTableStructFULL[num_row_i].type.str);
        strcat(tableStructOne->str, " ");

        if(strcmp("NO",mysqlTableStructFULL[num_row_i].null.str) == 0){
            strcat(tableStructOne->str, "NOT NULL ");
        }

        if(strcmp("NULL",mysqlTableStructFULL[num_row_i].defaultValue.str) != 0){
            strcat(tableStructOne->str,"DEFAULT '");
            strcat(tableStructOne->str, mysqlTableStructFULL[num_row_i].defaultValue.str);
            strcat(tableStructOne->str,"'");
        }

        strcat(tableStructOne->str, ",");
    }
    tableStructOne->str[strlen(tableStructOne->str) - 1] = 0;

    for(num_row_i = 0; num_row_i < num_row; num_row_i++) {
        if(strcmp("PRI", mysqlTableStructFULL[num_row_i].key.str) == 0) {
            if(flag == 0) {
                strcat(tableStructOne->str, ",PRIMARY KEY(");
                flag = 1;
            }
            strcat(tableStructOne->str,mysqlTableStructFULL[num_row_i].field.str);
            strcat(tableStructOne->str,",");
        }
    }

    if(flag == 1) {
        tableStructOne->str[strlen(tableStructOne->str) - 1] = 0;
        strcat(tableStructOne->str,")");
    }

    strcat(tableStructOne->str,")\n");
    tableStructOne->length = strlen(tableStructOne->str);
END:
    if(pMysqlResult != NULL) {
        mysql_free_result(pMysqlResult);
    }
    return ret;

}

int getIndexFromTable(MYSQL *pMySQL,tableName tableName,tableStruct *tableStructOne)
{
    MYSQL_RES *pMysqlResult = NULL;
    MYSQL_ROW row;

    mysqlIndexStruct *pMysqlIndexStruct = NULL;

    char sql[512] = { 0 };
    unsigned int num_row = 0;
    unsigned int num_row_i = 0;
    unsigned int num_col = 0;
    unsigned int num_fields = 0;

    int ret = 0;

    if(pMySQL == NULL || tableStructOne == NULL) {
        printf("function getIndexFromTable parameter error.\n");
        ret = -1;
        goto END;
    }

    memset(tableStructOne,0,sizeof(tableStruct));
    
    sprintf(sql,"SHOW INDEX FROM %s;\n",tableName.str);
    
    ret = mysql_real_query(pMySQL,sql,(unsigned int)strlen(sql));
    if(ret != 0) {
        printf("call mysql_real_query error.error = %s\n",mysql_error(pMySQL));
        ret = -4;
        goto END;
    }
    
    pMysqlResult = mysql_store_result(pMySQL);
    if(pMysqlResult == NULL) {
            printf("call mysql_store_result error.error = %s\n",mysql_error(pMySQL));
            ret = -4;
            goto END;
    }

    num_row = mysql_num_rows(pMysqlResult);
    if(num_row < 0) {
        printf("call mysql_num_rows error.error = %s\n",mysql_error(pMySQL));
        ret = -4;
        goto END;
    } else if (num_row == 0) {
        strcat(tableStructOne->str,"\n");
        ret = 0;
        goto END;
    }

    pMysqlIndexStruct = malloc(sizeof(mysqlIndexStruct)*num_row);    
    if(pMysqlIndexStruct == NULL) {
            printf("call malloc error.error = %s\n",strerror(errno));
            ret = -5;
            goto END;
    }

    num_fields = mysql_num_fields(pMysqlResult);
    while ((row = mysql_fetch_row(pMysqlResult))) {
        num_col = 0;
        unsigned long *lengths;
        lengths = mysql_fetch_lengths(pMysqlResult);
        
        pMysqlIndexStruct[num_row_i].table.length =  (int) lengths[num_col];
        strcpy(pMysqlIndexStruct[num_row_i].table.str,row[num_col]?row[num_col]:"NULL");
        num_col++;
        
        pMysqlIndexStruct[num_row_i].non_unique.length =  (int) lengths[num_col];
        strcpy(pMysqlIndexStruct[num_row_i].non_unique.str,row[num_col]?row[num_col]:"NULL");
        num_col++;
        
        pMysqlIndexStruct[num_row_i].key_name.length =  (int) lengths[num_col];
        strcpy(pMysqlIndexStruct[num_row_i].key_name.str,row[num_col]?row[num_col]:"NULL");
        num_col++;
        
        pMysqlIndexStruct[num_row_i].seq_in_index.length =  (int) lengths[num_col];
        strcpy(pMysqlIndexStruct[num_row_i].seq_in_index.str,row[num_col]?row[num_col]:"NULL");
        num_col++;
        
        pMysqlIndexStruct[num_row_i].column_name.length =  (int) lengths[num_col];
        strcpy(pMysqlIndexStruct[num_row_i].column_name.str,row[num_col]?row[num_col]:"NULL");
        num_col++;
        
        pMysqlIndexStruct[num_row_i].collation.length =  (int) lengths[num_col];
        strcpy(pMysqlIndexStruct[num_row_i].collation.str,row[num_col]?row[num_col]:"NULL");
        num_col++;
        
        pMysqlIndexStruct[num_row_i].cardinality.length =  (int) lengths[num_col];
        strcpy(pMysqlIndexStruct[num_row_i].cardinality.str,row[num_col]?row[num_col]:"NULL");
        num_col++;
        
        pMysqlIndexStruct[num_row_i].sub_part.length =  (int) lengths[num_col];
        strcpy(pMysqlIndexStruct[num_row_i].sub_part.str,row[num_col]?row[num_col]:"NULL");
        num_col++;
        
        pMysqlIndexStruct[num_row_i].packed.length =  (int) lengths[num_col];
        strcpy(pMysqlIndexStruct[num_row_i].packed.str,row[num_col]?row[num_col]:"NULL");
        num_col++;
        
        pMysqlIndexStruct[num_row_i].null.length =  (int) lengths[num_col];
        strcpy(pMysqlIndexStruct[num_row_i].null.str,row[num_col]?row[num_col]:"NULL");
        num_col++;
        
        pMysqlIndexStruct[num_row_i].index_type.length =  (int) lengths[num_col];
        strcpy(pMysqlIndexStruct[num_row_i].index_type.str,row[num_col]?row[num_col]:"NULL");
        num_col++;
        
        pMysqlIndexStruct[num_row_i].comment.length =  (int) lengths[num_col];
        strcpy(pMysqlIndexStruct[num_row_i].comment.str,row[num_col]?row[num_col]:"NULL");
        num_col++;

        num_row_i ++;
    }

    mysql_free_result(pMysqlResult);
    pMysqlResult = NULL;

    for(num_row_i = 0; num_row_i < num_row; num_row_i++) {
        if(strcmp("PRIMARY",pMysqlIndexStruct[num_row_i].key_name.str) == 0) {
                continue;
        }
        strcat(tableStructOne->str,"DROP INDEX IF EXISTS ");
        strcat(tableStructOne->str,pMysqlIndexStruct[num_row_i].key_name.str);
        strcat(tableStructOne->str,";");

        strcat(tableStructOne->str,"CREATE ");
        if(strcmp("0",pMysqlIndexStruct[num_row_i].non_unique.str) == 0) {
            strcat(tableStructOne->str,"UNIQUE ");
        }
        strcat(tableStructOne->str,"INDEX IF NOT EXISTS ");
        strcat(tableStructOne->str,pMysqlIndexStruct[num_row_i].key_name.str);
        strcat(tableStructOne->str," ");
        strcat(tableStructOne->str,"ON ");
        strcat(tableStructOne->str,pMysqlIndexStruct[num_row_i].table.str);
        strcat(tableStructOne->str,"(");
        strcat(tableStructOne->str,pMysqlIndexStruct[num_row_i].column_name.str);
        if(strcmp("A",pMysqlIndexStruct[num_row_i].collation.str) == 0) {
            strcat(tableStructOne->str," ASC");
        }
        strcat(tableStructOne->str,");\n");
    }

    tableStructOne->length = strlen(tableStructOne->str);

END:
    if(pMysqlIndexStruct != NULL) {
        free(pMysqlIndexStruct);
        pMysqlIndexStruct = NULL;
    }
    if(pMysqlResult != NULL) {
        mysql_free_result(pMysqlResult);
        pMysqlResult = NULL;
    }
    return ret;
}

int getDataRow(MYSQL *pMySQL,tableName tableNameOne,tableStruct *tableStructOne)
{
    static MYSQL_RES *pMysqlResult = NULL;
    static MYSQL_FIELD *fields = NULL;
    MYSQL_ROW row;
    unsigned long *lengths = NULL;

    static tableName staticTableName = {.length = 0,.str=""};

    char sql[512] = { 0 };
    static unsigned int num_row = 0;
    unsigned int num_col = 0;
    static unsigned int num_fields = 0;

    int ret = 0;

    if(pMySQL == NULL || tableStructOne == NULL) {
        printf("function getDataRow parameter error.\n");
        ret = -1;
        goto END;
    }

    memset(tableStructOne,0,sizeof(tableStruct));

    if(staticTableName.length == 0 || strcmp(staticTableName.str,tableNameOne.str) != 0) {
        if(pMysqlResult != NULL) {
            mysql_free_result(pMysqlResult);
            pMysqlResult = NULL;
        }

        sprintf(sql,"SELECT * FROM %s;",tableNameOne.str);
    
        ret = mysql_real_query(pMySQL,sql,(unsigned int)strlen(sql));
        if(ret != 0) {
            printf("call mysql_real_query error.error = %s\n",mysql_error(pMySQL));
            ret = -4;
            goto END;
        }
        
        pMysqlResult = mysql_store_result(pMySQL);
        if(pMysqlResult == NULL) {
                printf("call mysql_store_result error.error = %s\n",mysql_error(pMySQL));
                ret = -4;
                goto END;
        }

        memcpy(&staticTableName,&tableNameOne,sizeof(tableName));
        
        num_row = mysql_num_rows(pMysqlResult);
        if(num_row < 0) {
            printf("call mysql_num_rows error.error = %s\n",mysql_error(pMySQL));
            ret = -4;
            goto END;
        } else if (num_row == 0) {
            strcat(tableStructOne->str,"\n");
            ret = 0;
            goto END;
        }
        num_fields = mysql_num_fields(pMysqlResult);
        fields = mysql_fetch_fields(pMysqlResult);
    }

    row = mysql_fetch_row(pMysqlResult);
    if(row == NULL) {
        printf("call mysql_fetch_row error.error = %s\n",mysql_error(pMySQL));
        ret = 0;
        goto END;
    }

    sprintf(tableStructOne->str, "INSERT INTO %s(",tableNameOne.str);

    lengths = mysql_fetch_lengths(pMysqlResult);
    for(num_col = 0; num_col < num_fields; num_col++) {
        strcat(tableStructOne->str,fields[num_col].name);
        strcat(tableStructOne->str,",");
    }
    tableStructOne->str[strlen(tableStructOne->str) - 1] = ')';

    strcat(tableStructOne->str," VALUES (");
    for(num_col = 0; num_col < num_fields; num_col++) {
        if(row[num_col] == NULL) {
            strcat(tableStructOne->str,"NULL");
        } else {
            strcat(tableStructOne->str,"'");
            mysql_real_escape_string(pMySQL, tableStructOne->str + strlen(tableStructOne->str), row[num_col], strlen(row[num_col]));
            strcat(tableStructOne->str,"'");
        }
        strcat(tableStructOne->str,",");
    }
    tableStructOne->str[strlen(tableStructOne->str) - 1] = ')';
    strcat(tableStructOne->str,";");

    ret = num_row;
END:
    return ret;
}


int execSql(sqlite3 *pSqlite,char *str)
{
    char *errMsg = NULL;

    int ret = 0;

    if(pSqlite == NULL || str == NULL) {
        printf("function execSql parameter error.\n");
        ret = -1;
        goto END;
    }

    ret = sqlite3_exec(pSqlite,str,NULL,NULL,&errMsg);
    if(ret != SQLITE_OK) {
        printf("call sqlite3_exec error.error = %s\n",errMsg);
        printf("sql = %s \n",str);
        sqlite3_free(errMsg);
        ret = -3;
        goto END;
    }

END:
    return ret;

}
int createTable(sqlite3 *pSqlite,char *str)
{
    return execSql(pSqlite,str);
}

int deleteTable(sqlite3 *pSqlite,char *str)
{
    return execSql(pSqlite,str);
}

int createIndex(sqlite3 *pSqlite,char *str)
{
    return execSql(pSqlite,str);
}

int insertData(sqlite3 *pSqlite,char *str)
{
    return execSql(pSqlite,str);
}

int processDB(char *db_file,char *user,char *passwd,char *database)
{
    MYSQL *pMySQL = NULL;
    sqlite3 *pSqlite = NULL;
    char sql[2046] = { 0 };

    tableName tableNameArray[60];
    tableStruct tableStructOne;

    int num_table = 0;

    int i = 0;

    int ret = 0;

    memset(tableNameArray,0,sizeof(tableNameArray));
    memset(&tableStructOne,0,sizeof(tableStructOne));

    if(strlen(db_file) <= 0 || strlen(user) <= 0 || 
        strlen(passwd) <= 0 || strlen(database) <= 0) {
        printf("function processDB parameter error.\n");
        ret = -1;
        goto END;
    }

    pMySQL = mysql_init(NULL);
    if (pMySQL == NULL) {
        printf("call mysql_init error.error = %s\n",mysql_error(pMySQL));
        ret = -1;
        goto END;
    }

    if (!mysql_real_connect(pMySQL,"localhost",user,passwd,database,0,NULL,0)) {
        printf("call mysql_real_connect error.error = %s\n",mysql_error(pMySQL));
        ret = -2;
        goto END;
    }
    mysql_set_character_set(pMySQL, "utf8");

    ret = sqlite3_open(db_file, &pSqlite);
    if(ret != SQLITE_OK) {
        printf("call sqlite3_open error.error = %s\n",sqlite3_errmsg(pSqlite));
        ret = -3;
        goto END;
    }

    ret = getListTables(pMySQL,tableNameArray);
    if(ret < 0) {
        printf("call getListTables error.\n");
        ret = -4;
        goto END;
    }

    num_table = ret;

    for(i = 0; i < num_table; i++) {
        memset(&tableStructOne,0,sizeof(tableStructOne));
        ret = getTableStruct(pMySQL,tableNameArray[i],&tableStructOne);
        if(ret < 0) { 
            printf("call getTableStruct error.\n");
            ret = -4;
            goto END;
        }

        sprintf(sql,"DROP TABLE IF EXISTS %s;\n",tableNameArray[i].str);
        ret = deleteTable(pSqlite,sql);
        if(ret < 0) { 
            printf("call createTable error.\n");
            ret = -4;
            goto END;
        }

        ret = createTable(pSqlite,tableStructOne.str);
        if(ret < 0) { 
            printf("call createTable error.\n");
            ret = -4;
            goto END;
        }

        ret = getIndexFromTable(pMySQL, tableNameArray[i], &tableStructOne);
        if(ret < 0) { 
            printf("call createTable error.\n");
            ret = -4;
            goto END;
        }

        ret = createIndex(pSqlite,tableStructOne.str);
        if(ret < 0) { 
            printf("call createIndex error.\n");
            ret = -4;
            goto END;
        }

        for(; 1;){
            ret = getDataRow(pMySQL, tableNameArray[i], &tableStructOne);
            if(ret < 0) { 
                printf("call getDataRow error.\n");
                ret = -4;
                goto END;
            } else if(ret > 0) {
                ret = insertData(pSqlite,tableStructOne.str);
                if(ret < 0) { 
                    printf("call createIndex error.\n");
                    ret = -4;
                    goto END;
                }
            } else {
                break;
            }
        }
    }

END:
    if(pSqlite != NULL) {
        sqlite3_close(pSqlite);
        pSqlite = NULL;
    }
    if(pMySQL != NULL) {
        mysql_close(pMySQL);
        pMySQL = NULL;
    }
    return ret;
}

int usage(char *name)
{
    if(name == NULL){
        printf("usage: mysql2sqlite -d db file -u user -p passwd -b database name\n");
    } else {
        printf("usage: %s -d db file -u user -p passwd -b database name\n",name);
    }
    return 0;
}

int main(int argc,char *argv[])
{
    char opt = 0;
    char db_file[PATH_MAX] = { 0 };
    char user[512] ={ 0 };
    char passwd[512] = { 0 };
    char database[512] = { 0 };
    int ret = 0;

    while((opt = getopt(argc,argv,"d:u:p:b:")) != -1) {
        switch(opt) {
        case 'd':
            strcpy(db_file,optarg);
            break;
        case 'u':
            strcpy(user,optarg);
            break;
        case 'p':
            strcpy(passwd,optarg);
            break;
        case 'b':
            strcpy(database,optarg);
            break;
        case '?':
        default:
            usage(argv[0]);
            exit(EXIT_FAILURE);
            break;
        }
    }

    if(strlen(db_file) <= 0 || strlen(user) <= 0 || 
        strlen(passwd) <= 0 || strlen(database) <= 0) {
        usage(argv[0]);
        exit(EXIT_FAILURE);
    }

    ret = processDB(db_file,user,passwd,database);
    if(ret < 0) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
