/*
** LuaProfiler
** Copyright Kepler Project 2005-2007 (http://www.keplerproject.org/luaprofiler)
** $Id: stack.h,v 1.5 2007/08/22 19:23:53 carregal Exp $
*/

/*****************************************************************************
stack.h:
   Simple stack manipulation
*****************************************************************************/

#ifndef _STACK_H
#define _STACK_H

#include <time.h>

typedef struct lprofS_sSTACK_RECORD lprofS_STACK_RECORD;

struct lprofS_sSTACK_RECORD {
	double time_marker_function_local_time;
	double time_marker_function_total_time;
	char *file_defined;
	char *function_name;
	char *source_code;        
	long line_defined;
	long current_line;
	double local_time;
	double total_time;
	lprofS_STACK_RECORD *next;
};

typedef lprofS_STACK_RECORD *lprofS_STACK;

typedef struct lprofP_sSTATE lprofP_STATE;
	
struct lprofP_sSTATE {
   int stack_level;
   lprofS_STACK stack_top;
};

void lprofS_push(lprofS_STACK *p, lprofS_STACK_RECORD r);
lprofS_STACK_RECORD lprofS_pop(lprofS_STACK *p);

#endif
