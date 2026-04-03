ALTER SESSION SET query_tag = '{"origin":"sf_sit-is", "name":"Player_360", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":1, "source":"sql"}}';
USE ROLE "SF_APA_SANDBOX-QUICKSTARTS";
USE DATABASE QUICKSTARTS_DB;
USE SCHEMA PLAYER_360;

-- Account-level objects below may require additional privileges in a managed account.
-- If these fail, have your Snowflake admin create compute pools and external access integrations.

CREATE COMPUTE POOL IF NOT EXISTS PLAYER_360_cpu_xs_5_nodes
  MIN_NODES = 1
  MAX_NODES = 5
  INSTANCE_FAMILY = CPU_X64_XS;

CREATE COMPUTE POOL IF NOT EXISTS PLAYER_360_gpu_s_5_nodes
  MIN_NODES = 1
  MAX_NODES = 5
  INSTANCE_FAMILY = GPU_NV_S;

GRANT USAGE ON COMPUTE POOL PLAYER_360_cpu_xs_5_nodes TO ROLE "SF_APA_SANDBOX-QUICKSTARTS";
GRANT USAGE ON COMPUTE POOL PLAYER_360_gpu_s_5_nodes TO ROLE "SF_APA_SANDBOX-QUICKSTARTS";

CREATE OR REPLACE NETWORK RULE PLAYER_360_allow_all_rule
  TYPE = 'HOST_PORT'
  MODE= 'EGRESS'
  VALUE_LIST = ('0.0.0.0:443','0.0.0.0:80');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PLAYER_360_allow_all_integration
  ALLOWED_NETWORK_RULES = (PLAYER_360_allow_all_rule)
  ENABLED = true;

CREATE OR REPLACE NETWORK RULE PLAYER_360_pypi_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('pypi.org', 'pypi.python.org', 'pythonhosted.org',  'files.pythonhosted.org');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PLAYER_360_pypi_access_integration
  ALLOWED_NETWORK_RULES = (PLAYER_360_pypi_network_rule)
  ENABLED = true;

GRANT ALL PRIVILEGES ON INTEGRATION PLAYER_360_allow_all_integration TO ROLE "SF_APA_SANDBOX-QUICKSTARTS";
GRANT ALL PRIVILEGES ON INTEGRATION PLAYER_360_pypi_access_integration TO ROLE "SF_APA_SANDBOX-QUICKSTARTS";

USE WAREHOUSE SANDBOX_DB;
USE DATABASE QUICKSTARTS_DB;
USE SCHEMA PLAYER_360;

CREATE OR REPLACE NOTEBOOK QUICKSTARTS_DB.PLAYER_360.PLAYER_360_rolling_churn_prediction
FROM '@QUICKSTARTS_DB.PLAYER_360.notebook_rolling_churn_prediction'
MAIN_FILE = '1_Rolling_Churn_Prediction_Model.ipynb'
QUERY_WAREHOUSE = 'SANDBOX_DB'
COMPUTE_POOL='PLAYER_360_gpu_s_5_nodes'
RUNTIME_NAME='SYSTEM$GPU_RUNTIME';
ALTER NOTEBOOK QUICKSTARTS_DB.PLAYER_360.PLAYER_360_rolling_churn_prediction ADD LIVE VERSION FROM LAST;
ALTER NOTEBOOK QUICKSTARTS_DB.PLAYER_360.PLAYER_360_rolling_churn_prediction SET external_access_integrations = ("PLAYER_360_pypi_access_integration",
                                                                                                            "PLAYER_360_allow_all_integration");
