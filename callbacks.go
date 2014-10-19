package gohbase

/*
#include <stdlib.h>
#include <hbase/hbase.h>

void admin_dc_cb(int32_t err, hb_admin_t admin, void *extra)
{
	adminCloseCallback(err, admin, extra);
}

void cl_dsc_cb(int32_t err, hb_client_t client, void *extra)
{
	clientCloseCallback(err, client, extra);
}

void put_cb(int err, hb_client_t client, hb_mutation_t mutation,
            hb_result_t result, void *extra)
{
	putCallback(err, client, mutation, result, extra);
}

void get_send_cb(int32_t err, hb_client_t client, hb_get_t get, hb_result_t result, void *extra)
{
  	getCallback(err, client, get, result, extra);
}

void sn_cb(int32_t err, hb_scanner_t scan, hb_result_t *results, size_t numResults, void *extra)
{
  	scanNextCallback(err, scan, results, numResults, extra);
}

void sn_destroy_cb(int32_t err, hb_scanner_t scanner, void *extra)
{
  scanDestroyCallback(err, scanner, extra);
}
*/
import "C"

type CallbackResult struct {
  Results []*Result
  Err     error
}

func (r *CallbackResult) PrintAllResults() {
  for _, r := range r.Results {
    r.PrintResult()
  }
}
