USE ROLE "SF_APA_SANDBOX-QUICKSTARTS";
CREATE OR REPLACE STREAMLIT QUICKSTARTS_DB.PLAYER_360.PLAYER_360_streamlit
ROOT_LOCATION = '@QUICKSTARTS_DB.PLAYER_360.streamlit_player360'
MAIN_FILE = 'PLAYER_360.py'
QUERY_WAREHOUSE = 'SANDBOX_DB'
COMMENT = '{"origin":"sf_sit-is","name":"player_360","version":{"major":1, "minor":0},"attributes":{"is_quickstart":1, "source":"streamlit"}}';
