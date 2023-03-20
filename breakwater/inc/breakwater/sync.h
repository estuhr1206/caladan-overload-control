/*
 * sync.h - sync abstraction for overload control
 */

#pragma once

#include <base/lock.h>
#include <runtime/thread.h>
#include <runtime/sync.h>

bool mutex_lock_if_uncongested(mutex_t *m);
bool mutex_lock_is_congested(mutex_t *m);
bool is_slo_violated();

bool condvar_wait_if_uncongested(condvar_t *cv, mutex_t *m);
bool condvar_is_congested(condvar_t *cv);
