/*
** LuaProfiler
** Copyright Kepler Project 2005-2007 (http://www.keplerproject.org/luaprofiler)
** $Id: clocks.c,v 1.4 2007/08/22 19:23:53 carregal Exp $
*/

/*****************************************************************************
clocks.c:
   Module to register the time (seconds) between two events

Design:
   'lprofC_start_timer()' marks the first event
   'lprofC_get_seconds()' gives you the seconds elapsed since the timer
                          was started
*****************************************************************************/

#include <stdio.h>
#include "clocks.h"
#include <sys/time.h>

/*
   Here you can choose what time function you are going to use.
   These two defines ('TIMES' and 'CLOCK') correspond to the usage of
   functions times() and clock() respectively.
        Which one is better? It depends on your needs:
                TIMES - returns the clock ticks since the system was up
              (you may use it if you want to measure a system
              delay for a task, like time spent to get input from keyboard)
                CLOCK - returns the clock ticks dedicated to the program
                        (this should be prefered in a multithread system and is
              the default choice)

   note: I guess TIMES don't work for win32
*/

#ifdef TIMES

        #include <sys/times.h>

        static struct tms t;

        #define times(t) times(t)

#else /* ifdef CLOCK */

        #define times(t) clock()

#endif


#include <time.h>
double walltime(double t0)
{
  double t1;
  struct timeval  tv;
  struct timezone z;
  gettimeofday(&tv, &z);
  t1 = tv.tv_sec + tv.tv_usec * 1.0e-6;
  return t1 - t0;
}

void lprofC_start_timer(double *time_marker) {
  *time_marker = walltime(0.0);
}

static clock_t get_clocks(double time_marker) {
  return times(&t) - time_marker;
}

double lprofC_get_seconds(double time_marker) {
  return walltime(time_marker);
}

