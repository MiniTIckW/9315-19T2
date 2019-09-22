//CS9315 19T2 Assignment1
//date: 2019/6/30
//Group members: Yifan Wang, Jihui Liang, Zhiwei Wang
/*
 * src/tutorial/email.c
 *
 ******************************************************************************
  This file contains routines that can be bound to a Postgres backend and
  called by the backend in the process of processing queries.  The calling
  format for these routines is dictated by Postgres architecture.
******************************************************************************/

#include "postgres.h"

#include "fmgr.h"
#include "libpq/pqformat.h"

#include <string.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <regex.h>

PG_MODULE_MAGIC;

typedef struct email
{
    int length;
    char emailaddr[1];
}EmailAddr;


/*****************************************************************************
 * Input/Output functions
 *****************************************************************************/

Datum   email_in(PG_FUNCTION_ARGS);
Datum   email_out(PG_FUNCTION_ARGS);
Datum   email_recv(PG_FUNCTION_ARGS);
Datum   email_send(PG_FUNCTION_ARGS);
Datum   email_lt(PG_FUNCTION_ARGS);
Datum   email_le(PG_FUNCTION_ARGS);
Datum   email_eq(PG_FUNCTION_ARGS);
Datum   email_neq(PG_FUNCTION_ARGS);
Datum   email_gt(PG_FUNCTION_ARGS);
Datum   email_ge(PG_FUNCTION_ARGS);
Datum   email_deq(PG_FUNCTION_ARGS);
Datum   email_ndeq(PG_FUNCTION_ARGS);
Datum   email_cmp(PG_FUNCTION_ARGS);
int     isValidInput(char *str);

PG_FUNCTION_INFO_V1(email_in);

Datum email_in(PG_FUNCTION_ARGS)
{
    char *emad = PG_GETARG_CSTRING(0);
    int  i = 0;
    int  l = strlen(emad) + 1;
    EmailAddr *Addr = (EmailAddr *) palloc(l + VARHDRSZ);
    SET_VARSIZE(Addr,VARHDRSZ + l);

    //convert to connical form

    while(emad[i])
    {
        emad[i] = tolower(emad[i]);
        i++;
    }

    
    if (isValidInput(emad) == 0)
    {
        ereport( ERROR, (errcode (ERRCODE_INVALID_TEXT_REPRESENTATION),
            errmsg ("invalid input syntax for EmailAddress: \"%s\"", emad)));
    }
    
    memcpy(Addr->emailaddr, emad, l);

    PG_RETURN_POINTER(Addr);
}

int isValidInput(char *emad) 
{

    int  nbat  = 0;
    int  nbdm  = 0;
    int  i;
    char prev;
    int  check = 0;

    //the address must start with a letter 
    if (!isalpha(emad[0])) return 0;

    prev = emad[0];
    for (i = 1; emad[i]; i++) 
    {
        //check char is in range [a-z] && [0-9] && '-' && '@' && '.'
        if (!isalpha(emad[i]) && !isdigit(emad[i]) && emad[i] != '-' && emad[i] != '@' && emad[i] != '.') return 0;

        //takes care of empty words (e.g. j..shepherd@funny.email.com)
        if (emad[i] == '.' || emad[i] == '@' || emad[i] == '-')
        {
            if (!isalpha(prev) && !isdigit(prev)) 
            {   
                return 0;
            }
        }
        //compute the number of '@'
        if (emad[i] == '@') 
        { 
            check = i;
            nbat++;
            nbdm = 0;
        }
        //compute the number of '.' in Domain
        if (emad[i] == '.') 
        {
            nbdm++;
        }
        
        prev = emad[i];
    }

    if (!isalpha(prev) && !isdigit(prev)) return 0;
    //the number of @ must equal to 1
    if (nbat != 1) { printf("The number of @ is wrong.\n"); return 0; }
    //the number of words in domain must more than 1
    if (nbdm < 1) { printf("Not enough words in domain.\n"); return 0; }

    //max size of local(256) + domain(256) + '@' + NULL
    if (strlen(emad) > (256 + 256 + 1 + 1)) return 0;

    return check;
}

PG_FUNCTION_INFO_V1(email_out);

Datum email_out(PG_FUNCTION_ARGS)
{
    EmailAddr *email = (EmailAddr *) PG_GETARG_POINTER(0);
    int len = strlen(email->emailaddr) + 2;
    char *Addr = (char *)palloc(sizeof(char) * len);

    snprintf(Addr,sizeof(char) * len,"%s",email->emailaddr);
    PG_RETURN_CSTRING(Addr);
}

/*****************************************************************************
 * Binary Input/Output functions
 *
 * These are optional.
 *****************************************************************************/

PG_FUNCTION_INFO_V1(email_recv);

Datum email_recv(PG_FUNCTION_ARGS)
{
    StringInfo  buf = (StringInfo) PG_GETARG_POINTER(0);
    const char *emailaddr = pq_getmsgstring(buf);

    int len = strlen(emailaddr) + 1;
    EmailAddr *result = (EmailAddr *) palloc(VARHDRSZ + len);
	SET_VARSIZE(result,VARHDRSZ + len);

    memset(result->emailaddr,0,len);
    memcpy(result->emailaddr,emailaddr,len);

    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(email_send);

Datum email_send(PG_FUNCTION_ARGS)
{
    EmailAddr    *email = (EmailAddr *) PG_GETARG_POINTER(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendstring(&buf, email->emailaddr);

    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


/*****************************************************************************
 * Operator class for defining B-tree index
 *
 * It's essential that the comparison operators and support function for a
 * B-tree index opclass always agree on the relative ordering of any two
 * data values.  Experience has shown that it's depressingly easy to write
 * unintentionally inconsistent functions.  One way to reduce the odds of
 * making a mistake is to make all the functions simple wrappers around
 * an internal three-way-comparison function, as we do here.
 *****************************************************************************/

//#define Mag(c)  ((c)->local*(c)->local + (c)->domain*(c)->domain)

static int email_cmp_internal(EmailAddr * a, EmailAddr * b)
{
	char *domaina;
	char *domainb;
	char *locala;
	char *localb;
	char aaddr[strlen(a->emailaddr)+1]; 
	memcpy(aaddr,a->emailaddr,strlen(a->emailaddr)+1);
    char baddr[strlen(b->emailaddr)+1]; 
    memcpy(baddr,b->emailaddr,strlen(b->emailaddr)+1);
    locala = strtok(aaddr, "@");
    domaina = strtok(NULL, "@");
    localb = strtok(baddr, "@");
    domainb = strtok(NULL, "@");
    int result = 0;

    if (strcasecmp(domaina,domainb) < 0)
        result = -1;
    else if (strcasecmp(domaina,domainb) > 0)
        result = 1;
    else if (strcasecmp(domaina,domainb) == 0) 
    {
      if (strcasecmp(locala,localb) < 0)
         result = -1;
      else if (strcasecmp(locala,localb) > 0)
         result = 1;
    }
    return result;
}


int dom_cmp(EmailAddr * a, EmailAddr * b)
{
	char *domaina;
	char *domainb;
	char *locala;
	char *localb;
	char aaddr[strlen(a->emailaddr)+1]; 
	memcpy(aaddr,a->emailaddr,strlen(a->emailaddr)+1);
    char baddr[strlen(b->emailaddr)+1]; 
    memcpy(baddr,b->emailaddr,strlen(b->emailaddr)+1);
    locala = strtok(aaddr, "@");
    domaina = strtok(NULL, "@");
    localb = strtok(baddr, "@");
    domainb = strtok(NULL, "@");
	int result = 0;
    if (strcasecmp(domaina,domainb) < 0)
        result = -1;
    else if (strcasecmp(domaina,domainb) > 0)
        result = 1;

    return result;
}

PG_FUNCTION_INFO_V1(email_lt);

Datum email_lt(PG_FUNCTION_ARGS)
{
    EmailAddr    *a = (EmailAddr *) PG_GETARG_POINTER(0);
    EmailAddr    *b = (EmailAddr *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(email_cmp_internal(a, b) < 0);
}

PG_FUNCTION_INFO_V1(email_le);

Datum email_le(PG_FUNCTION_ARGS)
{
    EmailAddr    *a = (EmailAddr *) PG_GETARG_POINTER(0);
    EmailAddr    *b = (EmailAddr *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(email_cmp_internal(a, b) <= 0);
}

PG_FUNCTION_INFO_V1(email_eq);

Datum email_eq(PG_FUNCTION_ARGS)
{
    EmailAddr    *a = (EmailAddr *) PG_GETARG_POINTER(0);
    EmailAddr    *b = (EmailAddr *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(email_cmp_internal(a, b) == 0);
}

PG_FUNCTION_INFO_V1(email_neq);

Datum email_neq(PG_FUNCTION_ARGS)
{
    EmailAddr    *a = (EmailAddr *) PG_GETARG_POINTER(0);
    EmailAddr    *b = (EmailAddr *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(email_cmp_internal(a, b) != 0);
}

PG_FUNCTION_INFO_V1(email_ge);

Datum email_ge(PG_FUNCTION_ARGS)
{
    EmailAddr    *a = (EmailAddr *) PG_GETARG_POINTER(0);
    EmailAddr    *b = (EmailAddr *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(email_cmp_internal(a, b) >= 0);
}

PG_FUNCTION_INFO_V1(email_gt);

Datum email_gt(PG_FUNCTION_ARGS)
{
    EmailAddr    *a = (EmailAddr *) PG_GETARG_POINTER(0);
    EmailAddr    *b = (EmailAddr *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(email_cmp_internal(a, b) > 0);
}

PG_FUNCTION_INFO_V1(email_deq);

Datum email_deq(PG_FUNCTION_ARGS)
{
    EmailAddr    *a = (EmailAddr *) PG_GETARG_POINTER(0);
    EmailAddr    *b = (EmailAddr *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(dom_cmp(a,b) == 0);
}

PG_FUNCTION_INFO_V1(email_ndeq);

Datum email_ndeq(PG_FUNCTION_ARGS)
{
    EmailAddr    *a = (EmailAddr *) PG_GETARG_POINTER(0);
    EmailAddr    *b = (EmailAddr *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(dom_cmp(a,b) != 0);
}

PG_FUNCTION_INFO_V1(email_cmp);

Datum email_cmp(PG_FUNCTION_ARGS)
{
    EmailAddr    *a = (EmailAddr *) PG_GETARG_POINTER(0);
    EmailAddr    *b = (EmailAddr *) PG_GETARG_POINTER(1);

    PG_RETURN_INT32(email_cmp_internal(a, b));
}

PG_FUNCTION_INFO_V1(email_hash);

Datum email_hash(PG_FUNCTION_ARGS)
{	
	EmailAddr  *email = (EmailAddr *) PG_GETARG_POINTER(0);
	char	   *address;
	Datum		result;

    int len = strlen(email->emailaddr) + 1 + 1;
	address = (char *) palloc(sizeof(char) * len);

    snprintf(address, sizeof(char) * len, "%s", email->emailaddr);

    result = hash_any((unsigned char *) address, strlen(address));

	// pfree(address);

    PG_RETURN_DATUM(result);
    // PG_RETURN_INT32(DatumGetUInt32(hash_any((unsigned char *)address, strlen(address))));
}

