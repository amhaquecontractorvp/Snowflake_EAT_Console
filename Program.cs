using Snowflake.Data.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EATSnowflake
{
    class Program
    {
        static void Main(string[] args)
        {
            string spvFilePath = ConfigurationManager.AppSettings["SpvFileLocation"]; //"E:/ExpenseAllocation/SPV.txt";
            string connectionStrings = ConfigurationManager.AppSettings["ConnectionStrings"];// "account=hh43323.us-east-1;authenticator=snowflake_jwt;user=SVC_APP_DEV;password =XTP.bQYfpW4fhKiyYmdC;ROLE=DW_RO_DEV; private_key_file='E:/ExpenseAllocation/EAT_Snowflake/publickey/rsa_key_dev.p8'; db=VP_DW_DEV;schema=PUBLIC";
            string queryStatement = ConfigurationManager.AppSettings["QueryStatement"];//"select * from VP_DW_DEV.TSFA.VW_INVESTMENT_VEHICLE";
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                //conn.ConnectionString = "account=hh43323.us-east-1;authentcator=snowflake_jwt;user=svc_app;private_key_file='C:/Eat/publickey snow/rsa_key.p8'; db=VP_DW_PROD;schema=PUBLIC";
                conn.ConnectionString = connectionStrings;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                //cmd.CommandText = "select * from VP_DW_DEV.TSFA.VW_INVESTMENT_VEHICLE";
                cmd.CommandText = queryStatement;
                IDataReader reader = cmd.ExecuteReader();

                Dictionary<string, string> SpvData = new Dictionary<string, string>();

                while (reader.Read())
                {
                    SpvData.Add(reader.GetString(0), reader.GetString(1));
                }

                conn.Close();
                var pairs = from p in SpvData.Keys
                            select new { Key = p, Value = SpvData[p] };

                using (StreamWriter sw = new StreamWriter(spvFilePath))
                {
                    foreach (var pair in pairs)
                        sw.WriteLine("{0}|{1}", pair.Key, pair.Value);
                }

            }

        }
    }
}
