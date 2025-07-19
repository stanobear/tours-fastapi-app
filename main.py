from fastapi import FastAPI
from fastapi.responses import JSONResponse
import pyodbc
import pandas as pd
import json
from collections import defaultdict
import numpy as np
import uvicorn

app = FastAPI()

# --- Connection config (replace with real values) ---
DB_CONFIG = {
    "server": "EU-LATINT.data.tourplan.net,50275",
    "database": "EU-LATINT",
    "username": "excelEU-LATINT",
    "password": "y$WqsC8G+o4WxSub#9jZ"
}

# --- SQL query ---
SQL_QUERY = """
SELECT 

	 BHD.BHD_ID AS BOOKING_ID,BSL.BSL_ID AS SERVICE_ID,PXN.PXN_ID AS PAX_ID,PXN.PAX_FORENAME AS PAX_FORENAME,PXN.PAX_SURNAME AS PAX_SURNAME,PXN.PAX_TITLE AS PAX_TITLE,PXN.PAX_TYPE AS PAX_TYPE,PXN.CHILD_AGE AS PAX_CHILD_AGE
	,PXN.EDG_IND AS PAX_PAX_EDG,PXN.NOTES1 AS PAX_NOTES1,PXN.NOTES2 AS PAX_NOTES2
	,BHD.TRAVELDATE AS 'BOOKING_TRAVEL_DATE',BHD.AGENT,BHD.STATUS AS 'BOOKING_STATUS',BHD.CONSULTANT AS BOOKING_CONSULTANT,BHD.BOOKING_TYPE AS BOOKING_TYPE,BHD.BRANCH AS BOOKING_BRANCH
	,BHD.DEPARTMENT AS BOOKING_DEPARTMENT,BHD.REFERENCE AS BOOKING_REFERENCE,BHD.BOOKING_ORIGIN AS BOOKING_ORIGIN,BHD.FULL_REFERENCE AS BOOKING_FULL_REFERENCE
	,BSD.AGENT AS SERVICE_AGENT_PRICE,BSD.PAX AS SERVICE_PAX_NUMBER,BSD.SG AS SERVICE_SINGLES,BSD.TW AS SERVICE_TWINS,BSD.DB AS SERVICE_DOUBLES,BSD.TR AS SERVICE_TRIPLES,BSD.QD AS SERVICE_QUADS
	,BSD.ROOMCOUNT AS SERVICE_ROOMS_COUNT,BSD.CHD AS SERVICE_CHILDREN,BSD.INF AS SERVICE_INFANTS
	,BSL.[DAY] AS SERVICE_DAY_NUMBER,BSL.SEQ AS SERVICE_SEQUENCE,BSL.[DATE] AS 'SERVICE_DATE',BSL.SL_STATUS AS 'SERVICE_STATUS',BSL.PICKUP_DATE AS SERVICE_PICKUP_DATE
	,BSL.PICKUP AS SERVICE_PICKUP_POINT,BSL.DROPOFF_DATE AS SERVICE_DROPOFF_DATE,BSL.DROPOFF AS SERVICE_DROPOFF_POINT,BSL.REMARKS AS SERVICE_PUDO_REMARKS
	,BSL.VOUCHER_STATUS AS SERVICE_VOUCHER_STATUS,BSL.LIABILITY AS SERVICE_LIABILITY_FLAG,BSL.CREATED_BY AS SERVICE_CREATED_BY
	,OPT.CODE AS SERVICE_PRODUCT_CODE,OPT.[DESCRIPTION] AS SERVICE_PRODUCT_DESCRIPTION,OPT.COMMENT AS SERVICE_PRODUCT_COMMENT
	,'SERVICE_TOUR_NAME'=CASE WHEN OPT.CODE IN ('MARA25','MARL25') THEN 'MARAVILLAS' WHEN OPT.CODE IN ('PAIL25','PAIE25') THEN 'PAISAJES' WHEN OPT.CODE IN ('ESCO25') THEN 'ESCOCIA ROMANTICA' 
				WHEN OPT.CODE IN ('IRLT25') THEN 'IRLANDA TRADICIONAL' WHEN OPT.CODE IN ('IRAC25') THEN 'IRLANDA CLASSICA' WHEN OPT.CODE IN ('ENPB25') THEN 'ENCANTOS PAISAJES BAJOS'
				WHEN OPT.CODE IN ('MEJO25','MEJL25','LMPB25') THEN 'LO MEJOR' WHEN OPT.CODE IN ('TESO25') THEN 'TESOROS' WHEN OPT.CODE IN ('ICLL25','ICES25','INCL25','CIPB25') THEN 'INGLATERRA CLASICA'
				WHEN OPT.CODE IN ('GTDL25','GTDE25','GTPB25','GTCL25') THEN 'GRAN TOUR' WHEN OPT.CODE IN ('LEIR25','LEPB25') THEN 'LEYENDES' END
	
FROM
		 BSL
	JOIN BHD ON BHD.BHD_ID = BSL.BHD_ID
	JOIN BSD ON BSD.BSL_ID = BSL.BSL_ID
	JOIN OPT ON OPT.OPT_ID = BSL.OPT_ID
	JOIN PNB ON PNB.BHD_ID = BHD.BHD_ID
	JOIN PXN ON PNB.PXN_ID = PXN.PXN_ID

WHERE 
		BHD.TRAVELDATE BETWEEN '2025-01-01' AND '2025-12-31'
	AND OPT.SUPPLIER = 'ANGLOV'
	AND	OPT.CODE IN ('GTCL25','MARA25','MARL25','PAIL25','PAIE25','ESCO25','IRLT25','IRAC25','ENPB25','MEJO25','MEJL25','LEIR25','LEPB25','ICLL25','ICES25','INCL25','CIPB25','TESO25','GTDL25','GTPB25','GTDE25')
    AND BSL.LAST_WORK_DATE > '2025-07-16 00:00:00.000'
"""

# --- Helper functions ---
def convert_to_builtin_type(obj):
    if isinstance(obj, (np.generic, pd.Timestamp)):
        return obj.item() if hasattr(obj, 'item') else str(obj)
    return obj

def recursively_convert_types(data):
    if isinstance(data, dict):
        return {k: recursively_convert_types(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [recursively_convert_types(i) for i in data]
    else:
        return convert_to_builtin_type(data)

# --- Fetch and transform function ---
def fetch_and_transform_data():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']}"
    )
    conn = pyodbc.connect(conn_str)
    df = pd.read_sql(SQL_QUERY, conn)
    conn.close()

    # Format SERVICE_DATE
    if "SERVICE_DATE" in df.columns:
        df["SERVICE_DATE"] = pd.to_datetime(df["SERVICE_DATE"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Categorize columns
    booking_cols = [col for col in df.columns if col.startswith("BOOKING_")]
    service_cols = [col for col in df.columns if col.startswith("SERVICE_")]
    pax_cols = [col for col in df.columns if col.startswith("PAX_")]

    booking_id_col = "BOOKING_ID"
    service_id_col = "SERVICE_ID"
    pax_id_col = "PAX_ID"
    tour_name_col = "SERVICE_TOUR_NAME"

    # Build structure
    result = {"Tours": defaultdict(list)}

    for booking_id, booking_group in df.groupby(booking_id_col):
        booking_data = {
            booking_id_col: booking_id,
            **booking_group[booking_cols].iloc[0].to_dict()
        }

        services = []
        for service_id, service_group in booking_group.groupby(service_id_col):
            service_data = {
                service_id_col: service_id,
                **service_group[service_cols].iloc[0].to_dict()
            }

            tour_name = service_data.get(tour_name_col, "Unknown Tour")

            service_data["Pax"] = []
            for _, row in service_group.iterrows():
                pax_data = {col: row[col] for col in pax_cols}
                pax_data[pax_id_col] = row[pax_id_col]
                service_data["Pax"].append(pax_data)

            services.append(service_data)

        booking_data["Services"] = services
        result["Tours"][tour_name].append(booking_data)

    # Final type conversion to ensure JSON serialization
    result["Tours"] = recursively_convert_types(dict(result["Tours"]))
    return result

# --- API route ---
@app.get("/tours", response_class=JSONResponse)
def get_tours():
    try:
        data = fetch_and_transform_data()
        return JSONResponse(content=data, status_code=200)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)