from django.test import TestCase

# Create your tests here.
##import to model
from ios_input.ios_browser import get_ran_pi
from sla.models import SlaKpiModels
BH_KPI_MAP = {
    'Conn_SSR_With_LU_With_ImmAsgn': 'Erlang',
    'Accessibility_PS_GPRS_EGPRS_DL': 'PAYLOAD_LLC_TOTAL_MiB_MAPS',
    'Accessibility_PS_GPRS_EGPRS_UL': 'PAYLOAD_LLC_TOTAL_MiB_MAPS',
    'Throughput_EGPRS_LLC_DL_kbps': 'PAYLOAD_LLC_TOTAL_MiB_MAPS',
    'Throughput_GPRS_LLC_DL_kbps': 'PAYLOAD_LLC_TOTAL_MiB_MAPS',
    'HOSR_Out_GSM': 'Erlang',
    'HOSR_IRAT_Out': 'Erlang',
    'DCR': 'Erlang',
    'Quality_SamplesShare_0_To_4_DL': 'Erlang',
    'Quality_SamplesShare_0_To_4_UL': 'Erlang',
    'CSSR_CS': 'Erlang_3G',
    'CSSR_PS': 'Payload_Total_3G_GByte',
    'Throughput_EUL_NodeB_kbps': 'Payload_Total_3G_GByte',
    'Throughput_HS_DC_NodeB_kbps': 'Payload_Total_3G_GByte',
    'HOSR_Inter_Freq_CS_PS_Out': 'Erlang_3G',
    'RAB_Drop_Rate_CS': 'Erlang_3G',
    'RAB_Drop_Rate_HS': 'Payload_Total_3G_GByte',
    'RAB_Drop_Rate_EUL': 'Payload_Total_3G_GByte',
    'RTWP_Avg_Of_dBm_Values': 'Erlang_3G',
    'CSSR_Service': 'Payload_Total_4G_GByte',
    'ERAB_Setup_Succ_Rate_Added': 'Erlang_VoLTE',
    'Throughput_UE_QCI9_DL_kbps': 'Payload_Total_4G_GByte',
    'Throughput_UE_All_QCIs_UL_kbps': 'Payload_Total_4G_GByte',
    'HOSR_Intra_Freq_Out': 'Payload_Total_4G_GByte',
    'ERAB_Drop_Rate_Act_All_No_QCI5': 'Payload_Total_4G_GByte',
    'ERAB_Drop_Rate_VoLTE_QCI1': 'Erlang_VoLTE',
    'ERAB_Setup_Succ_Rate_QCI1': 'Erlang_VoLTE',
    'HOSR_Inter_Freq_Out': 'Payload_Total_4G_GByte',
    'SRVCC_Succ_Rate_UMTS_GSM': 'Erlang_VoLTE',
    'Latency_DL_Scheduling_msec': 'Payload_Total_4G_GByte',
}
KPI_LIST = {
    "GSM_SECTORNOTECH": [4570, 8335, 9107, 9108, 4082, 4084, 8338, 8341, 4045, 8374, 8377],
    "GSM_REGION": [4570, 8335, 9106, 9114, 4082, 4084, 8338, 8341, 4045, 8374, 8377],
    "UMTS_SECTORNOTECH": [4578, 4150, 4162, 4262, 4274, 4186, 4210, 4198, 8637, 9163],
    "UMTS_REGION": [4578, 4150, 4162, 4262, 4274, 4186, 4210, 4198, 9163, 8637],
    "LMBB_SECTORNOTECH": [5121, 20000, 4454, 6414, 4462, 4474, 4406, 4358, 4382, 5195, 4394, 8100, 4586],
    "LMBB_REGION": [5121, 20000, 4454, 6414, 4462, 4474, 4406, 4358, 4382, 5195, 4394, 8100, 4586],
    "LFBB_SECTORNOTECH": [5121, 20000, 4454, 4358, 4406, 4394, 4462, 4474],
    "LFBB_REGION": [5121, 20000, 4454, 4358, 4406, 4394, 4462, 4474],
    "NMBB_SECTORNOTECH": [5286, 4558, 4561, 9208, 9211, 8633, 9212, 5122, 8921],
    "NMBB_REGION": [5286, 4558, 4561, 9208, 9211, 8633, 5122, 8921]  # missing RB_UL_Interference (9212)
}
for tech in ['NMBB','GSM', 'UMTS', 'LMBB', 'LFBB']:
    vendor = 'Huawei' if tech=='NMBB' else 'Global'
    df = get_ran_pi(technology=tech, vendor=vendor, networks=['Irancell'])
    df1 = df[df['id'].apply(lambda y: True if y in set(KPI_LIST[f"{tech}_SECTORNOTECH"] + KPI_LIST[f"{tech}_REGION"]) else False)]
    df1 = df1[['id', 'name']]
    df1['technology'] = tech
    df1['bh_kpi_name'] = df1['name'].apply(lambda x: BH_KPI_MAP.get(x, None))
    df1['bh_kpi_id'] = df1['bh_kpi_name'].apply(
        lambda x: df[df['name'] == x]['id'].values[0] if not df[df['name'] == x]['id'].empty else None)
    if tech == 'NMBB':
        df1['bh_kpi_name'] = 'Payload_RLC_Total_GBYTE'
        df1['bh_kpi_id'] = '10008'
    for record in df1.to_dict('records'):
        item = SlaKpiModels(kpi_id=record['id'], name=record['name'], technology=record['technology'],
                            bh_kpi_id=record['bh_kpi_id'], bh_kpi_name=record['bh_kpi_name'])
        item.save()