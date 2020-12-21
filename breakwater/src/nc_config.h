/*
 * nc_config.h - NoControl configurations
 */

#pragma once

/* turn on AQM? */
#define SNC_AQM_ON		false
/* AQM Threshold */
#define SNC_AQM_THRESH		2000

/* Load Balancing Policy */
// Round-robin
#define CNC_LB_RR		1
// Random
#define CNC_LB_RAND		2

#define CNC_LB_POLICY		CNC_LB_RR
