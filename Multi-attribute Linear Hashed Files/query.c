// query.c ... query scan functions
// part of Multi-attribute Linear-hashed Files
// Manage creating and using Query objects
// Last modified by John Shepherd, July 2019

#include "defs.h"
#include "query.h"
#include "reln.h"
#include "tuple.h"
#include "hash.h"

// A suggestion ... you can change however you like

struct QueryRep 
{
	Reln rel;       // need to remember Relation info
	Bits known;     // the hash value from MAH
	Bits unknown;   // the unknown bits from MAH
	Bits stbucket;   // start bucket value
	Tuple qstring;
	int depth;
	Count count;      //count how many unknown bits in the certain depth

	PageID page_id;   // current page in scan
	int be_ovfl; // are we in the overflow pages?
	Offset pg_id;    // offset of current tuple within page
	Count nb_tups;     // number of tuples scanned in page_id
	Bits cmb_ukn;    // cur combination of unknown bits
};

// take a query string (e.g. "1234,?,abc,?")
// set up a QueryRep object for the scan

Query startQuery(Reln r, char *q)
{
	// TODO
	// Partial algorithm:
	// form known bits from known attributes
	// form unknown bits from '?' attributes
	// compute PageID of first page
	//   using known bits and first "unknown" value
	// set all values in QueryRep object

	Query new = malloc(sizeof(struct QueryRep));
	assert(new != NULL);
	new->rel = r;
	new->be_ovfl = 0;

	Count nvals = nattrs(r);
	//char *qu = strdup(q);
	new->qstring = strdup(q);
	//char *de = ",";
	char *vals[nvals];
	Bits hash[nvals];
	int i = 0;
	int attrknow[nvals];
	char buf[MAXBITS+1]; // for debug

	tupleVals(q,vals);
	new->pg_id = 0;
	new->nb_tups = 0;
	new->cmb_ukn = 0x00000000;
	while(i < nvals)
	{
		//vals[i] = strsep(&qu, de);
		if (vals[i] == NULL)
		{
			exit(0);
		}

		attrknow[i] = strcmp(vals[i], "?");
		if (!attrknow[i])
		{
			hash[i] = 0x00000000;
		} 
		else
		{
			hash[i] = hash_any((unsigned char *) vals[i], strlen(vals[i]));
		}

		bitsString(hash[i],buf);
		i++;
	}
	//free(qu);

	Bits qhash = 0xFFFFFFFF;
	Bits nknow = 0x00000000;
	ChVecItem *cv = chvec(r);
	Byte bit;
	Byte att;
	Bits mask = 0x00000000;
	//Not tested
	i = 0;
	while(i < MAXBITS)
	{
		mask = 0x00000000;
		att = cv[i].att;
		bit = cv[i].bit;
		if (!attrknow[att])
			nknow = setBit(nknow, i);
		mask = setBit(mask, bit);
		if ((hash[att] & mask) == 0)
			qhash = unsetBit(qhash, i);
		i++;
	}

	new->known = qhash;
	bitsString(qhash,buf);

	new->unknown = nknow;
	//showBits(nknow,buf);
	//printf("nknow is %s\n",buf);

	mask = 0x00000000;
	int d = depth(r);
	int j = 0;
	while(j < d)
	{
		mask = setBit(mask, j);
		j++;
	}
	//printf("%u\n",mask);
	PageID id = qhash & mask;
	new->depth = depth(r);
	if (id < splitp(r))
	{
		mask = setBit(mask, d);
		id = qhash & mask;
		new->depth++;
	}
	new->stbucket = id;
	new->page_id = id;

	int count = 0;
	int k = 0;
	while(k < MAXBITS-1)
	{
		if (k >= new->depth)
			break;
		if (nknow & (0x00000001 << k))
			count++;
		k++;
	}
	
	new->count = count;

//debug
//	showBits(id, buf);
//	printf("ID        %s\n", buf);
//	printf("start q OK\n");
	return new;
}

// get next tuple during a scan

Tuple getNextTuple(Query q)
{
	// TODO
	// Partial algorithm:
	// if (more tuples in current page)
	//    get next matching tuple from current page
	// else if (current page has overflow)
	//    move to overflow page
	//    grab first matching tuple from page
	// else
	//    move to "next" bucket
	//    grab first matching tuple from data page
	// endif
	// if (current page has no matching tuples)
	//    go to next page (try again)
	// endif

	Reln r = q->rel;
	while (1)
	{
		PageID pid = q->page_id;
		Page p;

		FILE *file;
		if (q->be_ovfl)
		{
			file = fovflow(r);
		} else
		{
			file = fdata(r);
		}

		p = getPage(file, pid);
		//scan the cur page until there is no left tuples
		//return if find match
		while (q->nb_tups < pageNTuples(p))
		{
			Tuple tmp = nextTuple(file,q->page_id,q->pg_id);
			q->nb_tups++;
			q->pg_id = q->pg_id + strlen(tmp) + 1;
			//printf("all tuples : %s   %d\n", tmp, strlen(tmp));	//debug
			if (tupleMatch(r, q->qstring, tmp))
				return tmp;
			free(tmp);
		}

		//switch to next page or overflow
		Offset ovflw = pageOvflow(p);
		if (ovflw != -1)
		{
			q->page_id = ovflw;
			q->nb_tups = 0;
			q->pg_id = 0;
			q->be_ovfl = 1;
			continue;
		} 
		else
		{
			Bits uknow = q->unknown;
			Bits op = q->cmb_ukn;
			op++;
			int i;
			int count = q->count;

			if (op >= (0x00000001 << count))
			{
				// including three cases:
				// 1) op from 0x11111111 to 0x00000000, count == 8
				// 2) op from 0x00000000 to 0x00000001, count == 0 (no unknown bits)
				// 3) op from 0x0000000F to 0x00000010, count == 4 (normal case)
				break;
			}
			q->cmb_ukn = op;

			int offset = 0;
			Bits mask = 0;
			for (i = 0; i < count; ++i)
			{
				while (!(uknow & (setBit(0, offset))))
					offset++;

				if (op & setBit(0, i))
					mask = setBit(mask, offset);

				offset++;
			}
			PageID id = q->stbucket | mask;

			if(id > npages(r)-1)
				break;
			//debug
//			char buf[35];
//			showBits(q->page_id, buf);
//			printf("from page\n    %s\n", buf);
//			showBits(id, buf);
//			printf("     to\n    %s\n", buf);

//
//			showBits(q->stbucket, buf);
//			printf("stbucket\n    %s\n ", buf);
//			showBits(mask, buf);
//			printf("mask\n    %s\n   op = %d    count = %d   depth = %d\n", buf,op,count,q->depth);

			q->page_id = id;
			q->nb_tups = 0;
			q->pg_id = 0;
			q->be_ovfl = 0;

		}
	}

	return NULL;
}

// clean up a QueryRep object and associated data

void closeQuery(Query q)
{
	// TODO
	free(q);
}
