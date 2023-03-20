/*
 * bw_config.h - Breakwater configurations
 */

#pragma once

/* Recommended parameters (in XL170 environment)
* - Memcached 25 / 50
* - 1 us average service time
* #define SBW_MIN_DELAY_US		40
* #define SBW_DROP_THRESH		80
*
* - 10 us average service time
* #define SBW_MIN_DELAY_US		80
* #define SBW_DROP_THRESH		160
*
* - 100 us average service time
* #define SBW_MIN_DELAY_US		150
* #define SBW_DROP_THRESH		300
*/

/* delay threshold for AQM */
#define SBW_LATENCY_BUDGET		55

#define SRPC_CM_SLOPE_THRESH		0.2
#define SRPC_CM_SLOPE_INV		4

#define SRPC_CM_UPDATE_INTERVAL		200
#define SRPC_CM_P99_RTT			100

/* round trip time in us */
#define SBW_RTT_US			10
#define SBW_AI				0.0005

/* the maximum supported window size */
#define SBW_MAX_WINDOW_EXP		6
#define SBW_MAX_WINDOW			64

#define SBW_MIN_WINDOW			0

#define CBW_MAX_CLIENT_DELAY_US		10
