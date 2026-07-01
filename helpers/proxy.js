import { Proxy } from "../models/proxyModel.js";

// TEMPORARY (testing): the hardcoded list below pre-seeds `proxies` so the scraper
// runs locally WITHOUT a seeded DB. In production, app.js calls loadProxies() at
// startup, which REPLACES this list with the MongoDB `proxies` collection.
// Mongo loaders (loadProxies / startProxyRefresh / stopProxyRefresh) are defined
// after the list, near the bottom of this file.

// Parse raw proxy strings in format: IP:PORT:USERNAME:PASSWORD
const rawProxies = [
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-w8ejzMhn_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-1g0k3q7l_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-2g5j8r9m_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-3h6k9s0n_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-4i7l0t1o_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-5j8m1u2p_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-LvFZ56Td_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-SLCRZmva_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-e7OZi7yh_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-nssHYsq4_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-xnhVHzSS_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-ql9SGpzv_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-mzvpFqDf_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-ZGqQRYqa_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-WgQtfdVo_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-w2xIeQrI_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-lowfvfsW_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-zTmAwyG0_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-MTVOtSUK_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-OkrExmqQ_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-Mt2SX1Ks_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-oAhs6Mzf_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-phb9KWoZ_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-AtMNLHkt_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-gG3vtjoW_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-4oQWSPqk_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-3oNsDIp3_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-t9OLmTG4_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-FDuIeY8v_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-jQlCQMmF_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-vjkRLhbH_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-fooarQ5Q_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-of1zDCsn_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-NuXW8Mri_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-iRy0kWKr_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-3gJ9Cdpc_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-2oLKwxtp_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-zNv65olI_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-fufEoa27_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-NERGYXew_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-djwclHkp_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-VplBGOPI_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-23qqDVAz_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-l1T902dC_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-KQ1RLo0i_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-p1v681kx_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-MQ0BFbFs_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-vLLIRde9_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-o7NDMcwZ_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-6PMeXCY4_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-t9xKY61v_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-d9nHJHDA_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-rtXyn7jo_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-wWpDvKnI_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-DJX5th0K_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-CddpJhWl_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-OtsEDXz0_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-pegHFLzj_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-55BNF9wQ_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-Zli0bdMA_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-LHYTs1dv_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-8jNmUwR3_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-BWcNG306_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-7igoAFpt_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-E2BDYMTH_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-VQNEOijJ_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-ZkObyWXQ_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-BwOQk4Ld_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-IQmxAGQq_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-PhQs9rCV_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-wsKGoAYI_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-k0fRAPkI_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-1sW6NetA_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-308PzOSX_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-WWA6jgaC_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-dK4KlDi8_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-5HDuIdfS_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-hEtr2z2I_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-dBO9mOOM_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-BfZkgaUc_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-aguytNbq_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-dkFaRlkK_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-a3LRDQiO_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-9Tptb5oW_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-xLi1Oide_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-zOHg7ewh_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-kaQaoMpy_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-kapPDItG_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-c2z0tE0h_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-P4VKq5VY_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-WnktqSlY_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-qUyQSKwD_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-4PdhyHH6_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-sOec4K9u_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-iM0G6RuY_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-2LHfN6Fz_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-ARkupFYm_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-ukbG8PP8_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-Lk6d9k2e_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-8qUe7kSh_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-Uu09BoQH_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-8cNqh8Vm_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-5YjluErQ_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-VKeMype8_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-4jCS0fGo_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-uvpzL4QM_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-p8XxnQcW_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-NSkOghIZ_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-OlW1cbYR_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-iYQ2AbjL_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-EGrpFKpA_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-V7PMSSYB_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-kiLdbWRt_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-cYSwYtQh_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-HpsQJrPF_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-gdYyHK1T_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-XAFR9OR8_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-ChijKanW_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-Sr1KS3gA_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-eoHV2dLW_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-58L7Cq47_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-S8VNauuY_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-cgbNomAM_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-e8JhAR85_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-3E7eEgmD_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-qFEK4aXU_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-JPjK5udP_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-BIswG0KA_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-tFmQ8Uv5_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-sV0LUs5F_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-SfaZspL0_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-5DOajcl1_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-WVhBFtdF_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-KlDcCs6z_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-otHFMhNP_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-6lyB0QUj_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-I3lPOy8U_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-bYQLImVU_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-Ktc9fMhI_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-ftQELaFo_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-VFVqJ2be_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-SNV7gbYx_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-zWVPGani_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-8AyiuJ05_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-Oqe0eQx5_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-jlPCX0IU_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-UnAG95Cr_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-xTPMta3D_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-BiRDfvLj_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-3Jo3PDR0_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-VNtZRgGz_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-2FkVrr02_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-nHCEWMlw_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-UuC1i4OL_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-10P9kQNv_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-28Bkw8Df_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-UNXhtg0B_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-oPQdgE1q_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-WjvkkW2i_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-TWoFLVY5_lifetime-30m",
"geo.iproyal.com:12321:eOmihbglu6jbMdlZ:XIdScuYoB6yNjVFv_country-us_session-5UZtbVtx_lifetime-30m",

];

// Live array — mutated in place so existing consumers (`proxyArray.proxies`) always
// see the latest list. Pre-seeded from the hardcoded list above so importing works
// without a DB (testing); loadProxies() replaces its contents from MongoDB in prod.
const proxies = rawProxies.map((rawProxy) => {
  const [ip, port, username, password] = rawProxy.split(":");
  return {
    proxy: `${ip}:${port}`,
    username: username,
    password: password,
  };
});

console.log(`[PROXY] Seeded ${proxies.length} hardcoded proxies (testing fallback; loadProxies() overrides from MongoDB in production)`);

const CLIENT_ID = process.env.CLIENT_ID || "default";
const REFRESH_INTERVAL_MS = parseInt(process.env.PROXY_REFRESH_INTERVAL_MS, 10) || 5 * 60 * 1000;
let refreshTimer = null;

// When true, IGNORE MongoDB entirely and use ONLY the hardcoded list above.
// Set USE_HARDCODED_PROXIES=1 (or =true) to test with these 50 without the DB
// overriding them. Default (unset) loads from MongoDB (production).
const USE_HARDCODED_PROXIES =
  process.env.USE_HARDCODED_PROXIES === "1" ||
  process.env.USE_HARDCODED_PROXIES === "true";

// ── IPRoyal rotating-residential mode ──────────────────────────────────────
// One credential, many STICKY sessions. Each session pins a stable residential
// IP for a whole homepage→event→facets flow (EPS's `tmpt` token is IP-bound, so
// the IP must NOT rotate mid-flow, or tmpt never mints and facets 403). The pool
// validates each session and binds the clean ones (~40% of residential IPs);
// flagged ones are skipped and a fresh session is spun. USE_IPROYAL=1 enables
// this instead of the hardcoded list / Mongo. All values overridable via env.
const USE_IPROYAL =
  process.env.USE_IPROYAL === "1" || process.env.USE_IPROYAL === "true";
const IPROYAL_HOST = process.env.IPROYAL_HOST || "geo.iproyal.com:12321";
const IPROYAL_USER = process.env.IPROYAL_USER || "eOmihbglu6jbMdlZ";
const IPROYAL_PASS = process.env.IPROYAL_PASS || "XIdScuYoB6yNjVFv_country-us";
const IPROYAL_SESSIONS = parseInt(process.env.IPROYAL_SESSIONS, 10) || 50;
const IPROYAL_LIFETIME = process.env.IPROYAL_LIFETIME || "30m";

function generateIproyalProxies(n = IPROYAL_SESSIONS) {
  const list = [];
  for (let i = 0; i < n; i++) {
    const session = Math.random().toString(36).slice(2, 10);
    list.push({
      id: `iproyal-${session}`, // unique key — all share the same host:port
      proxy: IPROYAL_HOST,
      username: IPROYAL_USER,
      password: `${IPROYAL_PASS}_session-${session}_lifetime-${IPROYAL_LIFETIME}`,
    });
  }
  return list;
}

// Expand a MongoDB proxy row into one or more pool entries. A rotating GATEWAY
// credential fans out into N sticky residential sessions — this is how DB mode
// supports rotating providers despite the unique (ip,port) index: ONE stored
// credential → many sessions, each with its own `id` so the pool can tell the
// sessions apart even though they all share one host:port. Two gateways are
// recognized, differing in WHERE the rotation token lives:
//   • IPRoyal      — host contains "iproyal", token in the PASSWORD
//                    (`_session-<t>_lifetime-<l>`).
//   • bartproxies  — host contains "bartproxies", token in the USERNAME
//                    (`_ss-<t>`); password constant.
// A pre-baked sticky row (token already present) and static IP proxies map 1:1.
function expandDbProxy(d) {
  const host = `${d.ip}:${d.port}`;
  const pw = d.password || "";
  const user = d.username || "";
  if (/iproyal/i.test(d.ip) && !/_session-/.test(pw)) {
    const list = [];
    for (let i = 0; i < IPROYAL_SESSIONS; i++) {
      const session = Math.random().toString(36).slice(2, 10);
      list.push({
        id: `iproyal-${session}`,
        proxy: host,
        username: d.username,
        password: `${pw}_session-${session}_lifetime-${IPROYAL_LIFETIME}`,
      });
    }
    return list;
  }
  // bartproxies gateway: rotation token lives in the USERNAME (`_ss-<token>`),
  // password constant. Each `_ss-` token pins its own exit IP (proven distinct
  // by scripts/bartSessions.mjs). Reuses IPROYAL_SESSIONS as the fan-out count.
  if (/bartproxies/i.test(d.ip) && !/_ss-/.test(user)) {
    const list = [];
    for (let i = 0; i < IPROYAL_SESSIONS; i++) {
      const session = Math.random().toString(36).slice(2, 10);
      list.push({
        id: `bart-${session}`,
        proxy: host,
        username: `${user}_ss-${session}`,
        password: pw,
      });
    }
    return list;
  }
  // Pre-baked sticky row (IPRoyal `_session-` or bartproxies `_ss-`) or static IP → 1:1.
  const sm = /_session-([^_]+)/.exec(pw) || /_ss-([^_]+)/.exec(user);
  return [{ id: sm ? `sticky-${sm[1]}` : host, proxy: host, username: d.username, password: pw }];
}

// Seed the live array with fresh sticky sessions immediately in IPRoyal mode.
if (USE_IPROYAL) {
  proxies.splice(0, proxies.length, ...generateIproyalProxies());
  console.log(`[PROXY] IPRoyal mode — ${proxies.length} sticky residential sessions via ${IPROYAL_HOST}`);
}

// Load proxies from MongoDB and REPLACE the in-memory list. Called by app.js at
// startup so production uses the DB. If the DB is empty or unreachable, the hardcoded
// fallback above is kept (so the scraper never silently ends up with zero proxies).
export async function loadProxies() {
  if (USE_IPROYAL) {
    // Regenerate sticky sessions on each (re)load → fresh residential IPs over time.
    proxies.splice(0, proxies.length, ...generateIproyalProxies());
    console.log(`[PROXY] IPRoyal mode — regenerated ${proxies.length} sticky sessions (fresh IPs)`);
    return proxies;
  }
  if (USE_HARDCODED_PROXIES) {
    console.log(`[PROXY] USE_HARDCODED_PROXIES set — using ${proxies.length} hardcoded proxies; NOT loading from MongoDB`);
    return proxies;
  }
  try {
    const docs = await Proxy.find({ clientId: CLIENT_ID, enabled: true }).lean();
    if (!docs.length) {
      console.warn(`[PROXY] MongoDB returned 0 proxies (clientId=${CLIENT_ID}); keeping ${proxies.length} hardcoded fallback proxies`);
      return proxies;
    }
    const next = docs.flatMap(expandDbProxy);
    proxies.splice(0, proxies.length, ...next);
    console.log(`[PROXY] Loaded ${docs.length} proxy row(s) from MongoDB → ${proxies.length} pool entries (clientId=${CLIENT_ID})`);
    return proxies;
  } catch (err) {
    console.error(`[PROXY] loadProxies failed (${err.message}); keeping ${proxies.length} hardcoded fallback proxies`);
    return proxies;
  }
}

export function startProxyRefresh() {
  if (USE_HARDCODED_PROXIES) {
    console.log("[PROXY] USE_HARDCODED_PROXIES set — periodic DB refresh disabled");
    return;
  }
  if (refreshTimer) return;
  refreshTimer = setInterval(() => {
    loadProxies().catch((err) => console.error("[PROXY] Refresh failed:", err.message));
  }, REFRESH_INTERVAL_MS);
  refreshTimer.unref?.();
}

export function stopProxyRefresh() {
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
}

export default {
  proxies: proxies,
};
