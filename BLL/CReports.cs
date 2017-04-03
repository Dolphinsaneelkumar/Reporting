using SqlDataAccess;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BLL
{
    public class CReports
    {
        public DataTable GetLatestSummaryAt()
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();
            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "SELECT max(SUMMARYAT)   SUMMARYAT from FOURHOURCOMPLAINTSUMMARY";
                _commnadData.OpenWithOutTrans();
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;
                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
             throw ex;
            }
            finally
            {
                 _commnadData.Close();

            }
        }

        public DataTable GetDistinctSummaryAt()
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "SELECT DISTINCT   SUMMARYAT from FOURHOURCOMPLAINTSUMMARY   ORDER   BY  SUMMARYAT   DESC";

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetDistinctSummaryAt_EightHourlyReport()
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "SELECT DISTINCT   SUMMARYAT from EIGHTHOURCOMPLAINTSUMMARY   ORDER   BY  SUMMARYAT   DESC";

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetDistinctCenter()
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "SELECT DISTINCT   MNCNAME from FOURHOURCOMPLAINTSUMMARY   ORDER   BY  MNCNAME   ASC";

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetDistinctCenter_EightHourlyReport()
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "SELECT DISTINCT   MNCNAME from EightHOURCOMPLAINTSUMMARY   ORDER   BY  MNCNAME   ASC";

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetDistinctMNCNameBySummaryAt(string summaryAt)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                //8/10/2016 11:15:04 AM
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "SELECT distinct MNCNAME from FOURHOURCOMPLAINTSUMMARY where   SummaryAt   =   To_Date('" + summaryAt + "', 'MM/DD/yyyy HH12:MI:ss AM')";

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetCRMTicketSummaryReport(string mncName, string summaryAt, string _dateFrom, string _dateTo)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "SELECT  " +
                    "  '" + mncName + "'    MNCNAME," +
"(SELECT    " +
"Count(*)   " +
"FROM    FOURHOURCOMPLAINTSUMMARY   " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND    " +
"           (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND     " +
"        (      " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss'))     OR     " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')      AND     TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))        " +
"        ))  PreviousBalance,   " +

"(SELECT        " +
"Count(*)       " +
"FROM    FOURHOURCOMPLAINTSUMMARY       " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND        " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND            " +
"        (          " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))     OR         " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))  AND (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))            " +
"))  NewReceived,       " +


"(SELECT    " +
"Count(*)   " +
"FROM    FOURHOURCOMPLAINTSUMMARY   " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND    " +
"           (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND     " +
"        (      " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss'))     OR     " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')      AND     TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))        " +
"        ))  +   " +
"(SELECT        " +
"Count(*)       " +
"FROM    FOURHOURCOMPLAINTSUMMARY       " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND        " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND            " +
"        (          " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))     OR         " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))  AND (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))            " +
"))  TOTALTICKET,       " +


"(SELECT                " +
"Count(*)               " +
"FROM    FOURHOURCOMPLAINTSUMMARY           " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND        " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND                    " +
"        (          " +

"            (STATUS    IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))       " +
"        )          " +
")   Resolved,         " +

"(SELECT                " +
"Count(*)               " +
"FROM    FOURHOURCOMPLAINTSUMMARY           " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND                " +
"        (          " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <=   TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))       " +

"        )      " +
")   TOTALPENDING,       " +

"(SELECT        " +
"Count(*)       " +
"FROM    FOURHOURCOMPLAINTSUMMARY       " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND         " +
"        (      " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <=   TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))       " +

"        )       AND        " +
"        (round(to_number(TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')  -   TO_DATE(ComplaintDateTime,  'yyyyMMddHH24MIss')) * 24)    >=  8)      " +
")   PENDINGGREATERTHAN8,           " +

"(SELECT                " +
"Count(*)               " +
"FROM    FOURHOURCOMPLAINTSUMMARY           " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND        " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND                " +
"            (STATUSREASON   LIKE    '%HT%')     AND            " +
"        (          " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <=   TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))           " +

"        )          " +
")   PENDINGHT ,     " +

"(SELECT                " +
"Count(*)               " +
"FROM    FOURHOURCOMPLAINTSUMMARY           " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND                " +
"        (          " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <=   TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))       " +

"        )      " +
")   -      " +

"(SELECT                " +
"Count(*)               " +
"FROM    FOURHOURCOMPLAINTSUMMARY           " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND        " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND                " +
"            (STATUSREASON   LIKE    '%HT%')     AND            " +
"        (          " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <=   TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))           " +

"        )          " +
")   LTPENDING      " +

"FROM    dual";

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetLTHTWireCableFaultReport(string mncName, string summaryAt, string _dateFrom, string _dateTo)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "SELECT  '" + mncName + "'   MNCNAME,    " +

"(SELECT  COUNT(*)      " +
"FROM    FOURHOURCOMPLAINTSUMMARY       " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND        " +
"           (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND          " +
"        (          " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss'))     OR     " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')      AND     TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))        " +
"        )       AND        " +
"        (UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR     " +
"           (StatusReason LIKE'F.I.R Case - Missed Wire Pulled%')       " +
"        )      " +
")   PREVIOUSBALANCELTWIREBROKEN,       " +

"(SELECT  Count(*)      " +
"FROM    FOURHOURCOMPLAINTSUMMARY       " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND            " +
"        (          " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))     OR         " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))  AND (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))        " +
"        )   AND        " +
"        (UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR     " +
"            (StatusReason LIKE'F.I.R Case - Missed Wire Pulled%')          " +
"        )          " +
")   NEWRECEIVEDLTWIREBROKEN,           " +

"((SELECT  COUNT(*)         " +
"FROM    FOURHOURCOMPLAINTSUMMARY           " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"           (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND          " +
"        (          " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss'))     OR     " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')      AND     TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))        " +
"        )       AND        " +
"        (UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR     " +
"            (StatusReason LIKE'F.I.R Case - Missed Wire Pulled%')      " +
"        )      " +
")   +      " +
"(SELECT  Count(*)      " +
"FROM    FOURHOURCOMPLAINTSUMMARY           " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND        " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND        " +
"        (      " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))     OR     " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))  AND (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))        " +
"        )   AND        " +
"        (UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR     " +
"            (StatusReason LIKE'F.I.R Case - Missed Wire Pulled%')      " +
"        )      " +
"))  TOTALLTWIREBROKEN,     " +

"(SELECT    Count(*)            " +
"FROM    FOURHOURCOMPLAINTSUMMARY       " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND        " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND            " +
"        (          " +

"           (STATUS    IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))            " +
"        )   AND            " +
"        (UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR         " +
"            (StatusReason LIKE'F.I.R Case - Missed Wire Pulled%')      " +
"        )      " +
")   RESOLVEDLTWIREBROKEN,          " +

"(((SELECT  COUNT(*)            " +
"FROM    FOURHOURCOMPLAINTSUMMARY           " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"           (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND          " +
"        (          " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss'))     OR         " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')      AND     TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))            " +
"        )       AND            " +
"        (UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR         " +
"            (StatusReason LIKE'F.I.R Case - Missed Wire Pulled%')          " +
"        )          " +
")   +          " +
"(SELECT  Count(*)          " +
"FROM    FOURHOURCOMPLAINTSUMMARY           " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND                " +
"        (          " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))     OR         " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))  AND (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))            " +
"        )   AND            " +
"        (UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR         " +
"            (StatusReason LIKE'F.I.R Case - Missed Wire Pulled%')          " +
"        )          " +
"))  -          " +
"((SELECT    Count(*)           " +
"FROM    FOURHOURCOMPLAINTSUMMARY           " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND                " +
"        (              " +

"            (STATUS    IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))           " +
"        )   AND            " +
"        (UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR         " +
"            (StatusReason LIKE'F.I.R Case - Missed Wire Pulled%')          " +
"        )          " +
"))         " +
")   PENDINGLTWIREBROKEN,           " +

"(SELECT            " +
"Count(*)           " +
"FROM    FOURHOURCOMPLAINTSUMMARY           " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"           (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND              " +
"        (          " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss'))     OR         " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')      AND     TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))            " +
"        )       AND            " +
"        (UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
"        )          " +
")   PREVIOUSBALANCEHTWIREBROKEN,           " +

"(          " +
"SELECT             " +
"Count(*)           " +
"FROM    FOURHOURCOMPLAINTSUMMARY           " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND            " +
"        (              " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))     OR         " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))  AND (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))            " +
"        )   AND            " +
"        ((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'         " +
"        )              " +

"))   NEWRECEIVEDHTWIREBROKEN,              " +

"(              " +
"(SELECT            " +
"Count(*)           " +
"FROM    FOURHOURCOMPLAINTSUMMARY           " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"           (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND          " +
"       (               " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss'))     OR         " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')      AND     TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))            " +
"        )       AND            " +
"        (UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
"        )          " +
")       +          " +
"(              " +
"SELECT         " +
"Count(*)       " +
"FROM    FOURHOURCOMPLAINTSUMMARY           " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND        " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND        " +
"        (      " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss'))     OR         " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss'))  AND (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')))            " +
"        )   AND            " +
"        ((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'         " +
"        )          " +

"))         " +
")       TOTALHTWIREBROKEN,         " +

"(      " +
"SELECT             " +
"Count(*)           " +
"FROM    FOURHOURCOMPLAINTSUMMARY       " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND        " +
"           (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND                     " +
"        (              " +

"            (STATUS    IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))           " +
"        )   AND                " +
"        ((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'             " +
"        )              " +

"))      RESOLVEDHTWIREBROKEN,              " +

"(              " +
"(              " +
"(SELECT        " +
"Count(*)       " +
"FROM    FOURHOURCOMPLAINTSUMMARY           " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"           (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND              " +
"        (              " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss'))     OR             " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')      AND     TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))            " +
"        )       AND                " +
"        (UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
"        )          " +
")       +          " +
"(          " +
"SELECT         " +
"Count(*)       " +
"FROM    FOURHOURCOMPLAINTSUMMARY           " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND                " +
"        (              " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))     OR     " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))  AND (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))        " +
"        )   AND            " +
"        ((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'         " +
"        )          " +

"))         " +
")   -      " +
"(          " +
"SELECT             " +
"Count(*)       " +
"FROM    FOURHOURCOMPLAINTSUMMARY           " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND        " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND            " +
"        (          " +

"            (STATUS    IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))           " +
"        )   AND            " +
"        ((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'         " +
"        )          " +

"))             " +
")       PENDINGHTWIREBROKEN,           " +

"(              " +
"SELECT   Count(*)          " +
"FROM    FOURHOURCOMPLAINTSUMMARY           " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND        " +
"           (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND          " +
"        (              " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss'))     OR         " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')      AND     TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))            " +
"        )       AND            " +
"        (UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'            " +
"        )          " +

")       PREVIOUSBALANCELTCABLEFAULT,           " +

"(          " +
"SELECT         " +
"Count(*)       " +
"FROM    FOURHOURCOMPLAINTSUMMARY       " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND            " +
"        (          " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))     OR         " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))  AND (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))            " +
"        )   AND            " +
"        ((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'       " +
"        )      " +

"))      NEWRECEIVEDLTCABLEFAULT,           " +

"(          " +
"(          " +
"SELECT   Count(*)          " +
"FROM    FOURHOURCOMPLAINTSUMMARY           " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"           (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND      " +
"        (          " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss'))     OR             " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')      AND     TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))            " +
"        )       AND            " +
"        (UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'            " +
"        )          " +

")   +          " +
"(              " +
"SELECT         " +
"Count(*)       " +
"FROM    FOURHOURCOMPLAINTSUMMARY       " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND            " +
"        (          " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))     OR         " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))  AND (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))            " +
"        )   AND            " +
"        ((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
"        )          " +

"))         " +
")       TOTALLTCABLEFAULT,     " +

"(          " +
"SELECT             " +
"Count(*)           " +
"FROM    FOURHOURCOMPLAINTSUMMARY       " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND            " +
"        (          " +

"            (STATUS    IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))           " +
"        )   AND            " +
"        ((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
"        )          " +
"))       RESOLVEDLTCABLEFAULT,         " +

"(          " +
"(          " +
"(          " +
"SELECT   Count(*)          " +
"FROM    FOURHOURCOMPLAINTSUMMARY           " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"           (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND          " +
"        (          " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss'))     OR     " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')      AND     TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))        " +
"        )       AND        " +
"        (UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'        " +
"        )      " +

")   +      " +
"(      " +
"SELECT         " +
"Count(*)       " +
"FROM    FOURHOURCOMPLAINTSUMMARY       " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND        " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND        " +
"        (      " +
"            (STATUS     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))     OR     " +
"            (STATUS             IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))  AND (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))        " +
"        )   AND        " +
"        ((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'       " +
"        )          " +

"))     " +
")   -      " +
"(      " +
"SELECT         " +
"Count(*)       " +
"FROM    FOURHOURCOMPLAINTSUMMARY       " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"            (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND            " +
"        (          " +

"            (STATUS    IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))           " +
"        )   AND        " +
"        ((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
"        )          " +
"))         " +
")       PENDINGLTCABLEFAULT            " +

"FROM    dual";

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetOPNDetailReport(string mncName, string summaryAt, string _dateFrom, string _dateTo)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;

                //                _commnadData.CommandText = "Select  '" + mncName + "'   MNCNAME,    C.DTSID,    C.FEEDERNAME,   '' TrafoName,   '' AffectedArea,    " +
                //"TO_DATE(C.StatusDateTime,  'yyyyMMddHH24MIss') ForwardDate,CT.ComplaintTypeName FaultDescription ,          " +
                //"(select Count(Comp.PARENTTICKETNO)          " +
                //"from FOURHOURCOMPLAINTSUMMARY Comp where Comp.PARENTTICKETNO    =   C.CRMSERVICETICKETNO )  CHILDTICKET             " +
                //"from FOURHOURCOMPLAINTSUMMARY C             " +
                //"inner join ComplaintType  CT  on C.ComplaintType    = CT.ComplaintTypeCode          " +
                //"Where (C.MNCNAME   =   '" + mncName + "')      AND         " +
                //"        (C.StatusTo='OPN')          AND         " +
                //"         (C.ParentTicketNo is null OR C.ParentTicketNo='0')     AND         " +
                //"         (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))     AND         " +
                //"TO_DATE(C.ComplaintDateTime,'yyyyMMddHH24MIss')  between TO_DATE('" + _dateFrom + "','yyyyMMddHH24MIss') AND TO_DATE('" + _dateTo + "','yyyyMMddHH24MIss')     ";


                _commnadData.CommandText = "Select  '" + mncName + "'   MNCNAME,    C.DTSID,    C.FEEDERNAME,   '' TrafoName,   '' AffectedArea,    " +
"TO_DATE(C.StatusDateTime,  'yyyyMMddHH24MIss') ForwardDate,CT.ComplaintTypeName FaultDescription ,          " +
"(select Count(Comp.PARENTTICKETNO)          " +
"from FOURHOURCOMPLAINTSUMMARY Comp where Comp.PARENTTICKETNO    =   C.CRMSERVICETICKETNO )  CHILDTICKET             " +
"from FOURHOURCOMPLAINTSUMMARY C             " +
"inner join ComplaintType  CT  on C.ComplaintType    = CT.ComplaintTypeCode          " +
"Where (C.MNCNAME   =   '" + mncName + "')      AND         " +
"        (C.StatusTo='OPN')          AND         " +
"         (C.ParentTicketNo is null OR C.ParentTicketNo='0')     AND         " +
"         (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM')) ";

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetLTPendingTicketsDetails(string mncName, string summaryAt, string _dateFrom, string _dateTo)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;

                //                _commnadData.CommandText = "SELECT '" + mncName + "'    MNCNAME,    CT.ComplaintTypeName FaultType, Count(C.ComplaintId) NOOFTICKET     " +
                //"FROM     FOURHOURCOMPLAINTSUMMARY C        " +
                //"inner join ComplaintType  CT  on C.ComplaintType= CT.ComplaintTypeCode         " +
                //"Where   (C.MNCName  =   '" + mncName + "')      AND       " +
                //"        ((C.ParentTicketNo is null OR C.ParentTicketNo='0'))        AND        " +
                //"        (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND        " +
                //"TO_DATE(C.ComplaintDateTime,'yyyyMMddHH24MIss') between TO_DATE('" + _dateFrom + "','yyyyMMddHH24MIss') AND TO_DATE('" + _dateTo + "','yyyyMMddHH24MIss')          " +
                //"   Group by MNCNAME ,CT.ComplaintTypeName ";



                _commnadData.CommandText = "SELECT '" + mncName + "'    MNCNAME,    CT.ComplaintTypeName FaultType, Count(C.ComplaintId) NOOFTICKET     " +
"FROM     FOURHOURCOMPLAINTSUMMARY C        " +
"inner join ComplaintType  CT  on C.ComplaintType= CT.ComplaintTypeCode         " +
"Where   (C.MNCName  =   '" + mncName + "')      AND       " +
"        ((C.ParentTicketNo is null OR C.ParentTicketNo='0'))        AND        " +
"        (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))  " +
"   Group by MNCNAME ,CT.ComplaintTypeName ";


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable Get08HourlyLTFaultsDetails(string mncName, string summaryAt, string _dateFrom, string _dateTo)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;

                //                _commnadData.CommandText = "SELECT '" + mncName + "'    MNCNAME,    CT.ComplaintTypeName FaultType, Count(C.ComplaintId) NOOFTICKET     " +
                //"FROM     FOURHOURCOMPLAINTSUMMARY C        " +
                //"inner join ComplaintType  CT  on C.ComplaintType= CT.ComplaintTypeCode         " +
                //"Where   (C.MNCName  =   '" + mncName + "')      AND       " +
                //"        ((C.ParentTicketNo is null OR C.ParentTicketNo='0'))        AND        " +
                //"        (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND        " +
                //"TO_DATE(C.ComplaintDateTime,'yyyyMMddHH24MIss') between TO_DATE('" + _dateFrom + "','yyyyMMddHH24MIss') AND TO_DATE('" + _dateTo + "','yyyyMMddHH24MIss')          " +
                //"   Group by MNCNAME ,CT.ComplaintTypeName ";



                _commnadData.CommandText = "SELECT      DISTINCT    MNCNAME,    Statusreason,   " +

"(SELECT   Count(*)         " +
"FROM    EIGHTHOURCOMPLAINTSUMMARY   ee     " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"          (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))                  AND        " +
"          ((e.StatusReason   =   ee.StatusReason)  OR      (ee.StatusReason    IS  NULL   AND e.StatusReason   IS  NULL) )     AND     " +
"        (          " +
"            (TRIM(STATUS)     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss'))     OR       " +
"            (TRIM(STATUS)             IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')      AND     TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))      " +
"        )      " +
")   PREVIOUSBALANCE,       " +


"(SELECT   Count(*)     " +
"FROM    EIGHTHOURCOMPLAINTSUMMARY   ee     " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"        (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND              " +
"        ((e.StatusReason   =   ee.StatusReason)  OR      (ee.StatusReason    IS  NULL   AND e.StatusReason   IS  NULL) )     AND           " +
"        (              " +
"            (TRIM(STATUS)     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))     OR       " +
 "           (TRIM(STATUS)             IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))  AND (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('20160811024955',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))      " +
  "      )      " +
")   NEWFAULTSREPORTED,     " +


"(SELECT   Count(*)         " +
"FROM    EIGHTHOURCOMPLAINTSUMMARY   ee         " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"          (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))                  AND            " +
"          ((e.StatusReason   =   ee.StatusReason)  OR      (ee.StatusReason    IS  NULL   AND e.StatusReason   IS  NULL) )     AND         " +
"        (                  " +
"            (TRIM(STATUS)     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss'))     OR           " +
"            (TRIM(STATUS)             IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')      AND     TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))          " +
"        )          " +
")   +          " +


"(SELECT   Count(*)         " +
"FROM    EIGHTHOURCOMPLAINTSUMMARY   ee         " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"        (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND              " +
"        ((e.StatusReason   =   ee.StatusReason)  OR      (ee.StatusReason    IS  NULL   AND e.StatusReason   IS  NULL) )     AND               " +
"        (                  " +
"            (TRIM(STATUS)     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))     OR           " +
"            (TRIM(STATUS)             IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))  AND (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))          " +
"        )          " +
")   TOTALFAULTS,           " +


"(SELECT   Count(*)         " +
"FROM    EIGHTHOURCOMPLAINTSUMMARY   ee             " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND                " +
"        (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND              " +
"        ((TRIM(e.StatusReason)   IN   TRIM(ee.StatusReason))     OR      (ee.StatusReason    IS  NULL   AND e.StatusReason   IS  NULL) )         AND           " +
"        (            " +

"            (TRIM(STATUS)    IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))         " +
"        )            " +
")   FAULTSREPAIRED,            " +


"(SELECT   Count(*)             " +
"FROM    EIGHTHOURCOMPLAINTSUMMARY   ee             " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"          (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))                  AND            " +
"          ((e.StatusReason   =   ee.StatusReason)  OR      (ee.StatusReason    IS  NULL   AND e.StatusReason   IS  NULL) )     AND             " +
"        (                  " +
 "           (TRIM(STATUS)     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss'))     OR       " +
"            (TRIM(STATUS)             IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') <   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')      AND     TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))          " +
 "       )          " +
")   +          " +


"(SELECT   Count(*)         " +
"FROM    EIGHTHOURCOMPLAINTSUMMARY   ee         " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
"        (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND          " +
"        ((e.StatusReason   =   ee.StatusReason)  OR      (ee.StatusReason    IS  NULL   AND e.StatusReason   IS  NULL) )     AND           " +
 "       (                  " +
"            (TRIM(STATUS)     NOT     IN  ('E0005',   'E0006')    AND     TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))     OR           " +
"            (TRIM(STATUS)             IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTDATETIME,  'yyyyMMddHH24MIss') BETWEEN   TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')    AND     TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss'))  AND (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))          " +
"        )          " +
")   -              " +

"(SELECT   Count(*)             " +
"FROM    EIGHTHOURCOMPLAINTSUMMARY   ee             " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
 "       (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND              " +
"        ((TRIM(e.StatusReason)   IN   TRIM(ee.StatusReason))     OR      (ee.StatusReason    IS  NULL   AND e.StatusReason   IS  NULL) )         AND           " +
 "       (            " +

"            (TRIM(STATUS)    IN  ('E0005',   'E0006')    AND     (TO_DATE(COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')  BETWEEN     TO_DATE('" + _dateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')))         " +
"        )                " +
")   FAULTSPENDING,         " +

"''  REMARKS            " +


"FROM    EIGHTHOURCOMPLAINTSUMMARY       e          " +
"WHERE   ((PARENTTICKETNO IS  NULL    OR  PARENTTICKETNO  =   0)  AND     MNCNAME =   '" + mncName + "')    AND            " +
 "         (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))             " +
"ORDER   BY  MNCNAME,    StatusREASON";


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        #region Manager Main Page...

        public DataTable getMncByDepartmentName(string deptName)
        {
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "select C.MncCode,C.MncName from centre C inner join Department D on C.DeptId=D.deptId where D.DeptName='" + deptName + "'"; ;

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }
        public DataTable getDutyRoosterResourceInfo(string MncName)
        {
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "SELECT DR.CentrE, SI.USERFULLNAME ShiftInchage,SS.USERFULLNAME SrSupervisor,CC.USERFULLNAME ComplaintCoordinate,DR.ShiftName,DR.SHiftDate,DR.ShiftStartTime,DR.ShiftEndTime,Count(DRT.MTL) TeamCount,LISTAGG(TRIM(DRT.GangName), ',') WITHIN GROUP (ORDER BY DRT.GangName) GangName,CN.MNCCODE, SI.UserID SHIFTINCHARGEID,SS.UserID SUPERVISORELECTRICALID,CC.UserID COMPLAINTCOORDINATORID FROM DUTYROOSTER DR   inner join Centre Cn on Dr.Centre=Cn.MncName inner join Users SI on  DR.SHIFTINCHARGEID=SI.UserId inner join Users SS on  DR.SUPERVISORELECTRICALID=SS.UserId inner join Users CC on  DR.COMPLAINTCOORDINATORID=CC.UserId inner join Shift S on Dr.ShiftName=S.shiftName INNER JOIN DutyRoosterTeam DRT on   Dr.DutyRoosterId= DRT.DutyRoosterId   WHERE  DR.Centre in(" + MncName + ") AND DR.SHIFTACTIVE=1 AND  SHIFTSIGNINTIME IS NOT NULL   Group BY  DR.CentrE, SI.USERFULLNAME,SS.USERFULLNAME,CC.USERFULLNAME,DR.ShiftName,DR.SHiftDate,DR.ShiftStartTime,DR.ShiftEndTime,SI.UserID,SS.UserID,CC.UserID ,CN.MNCCODE";

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable getComplaintByDepartment(string mncCode, string currentSHiftDateFrom, string currentShiftDateTO)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "select '' FaultName,   S.StatusColor,  S.StatusName, C.ComplaintTypeCode   ComplaintTypeName, To_Date(C.ComplaintDateTime,'yyyyMMddHH24MIss') ComplaintDateTime, To_Date(C.StatusDateTime,'yyyyMMddHH24MIss') StatusDateTime,To_Date(C.ComplaintAttemptDateTime,'yyyyMMddHH24MIss') ComplaintAttemptDateTime,  C.*  from Complaint C  " +
                                                   " inner join Status S on C.Status=S.StatusCode and S.StatusCode !='E0002'  " +
                                                    "where        (C.MNCCODE in(" + mncCode + ") AND  trim(C.ComplaintDateTime) !='0' AND C.ComplaintDateTime is not  null     AND  " +
                                                    " (    (C.Status   NOT IN  ('E0005',   'E0006') AND  trim(C.ComplaintDateTime) !='0' AND C.ComplaintDateTime is not null   AND     TO_DATE(C.ComplaintDateTime,  'yyyyMMddHH24MIss') < TO_DATE('" + currentSHiftDateFrom + "',  'yyyyMMddHH24MIss'))   OR    " +
                                                     " (C.STATUS   NOT IN  ('E0005',   'E0006')    AND    trim(C.ComplaintDateTime) !='0' AND C.ComplaintDateTime is not  null AND    TO_DATE(C.ComplaintDateTime,  'yyyyMMddHH24MIss')   BETWEEN     TO_DATE('" + currentSHiftDateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + currentShiftDateTO + "',  'yyyyMMddHH24MIss'))     OR  " +
                                                      "  (C.STATUS    IN  ('E0005',   'E0006')    AND        TO_DATE(C.COMPLAINTATTEMPTDATETIME,  'yyyyMMddHH24MIss')   BETWEEN     TO_DATE('" + currentSHiftDateFrom + "',  'yyyyMMddHH24MIss')  AND TO_DATE('" + currentShiftDateTO + "',  'yyyyMMddHH24MIss')))) order by C.ComplaintId DESC";

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        #endregion

        #region LT Fault Report....

        public DataTable getCRMCodeByOPNOrUGMOrMM()
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;



                _commnadData.CommandText = "select distinct ST.ToWhom,SR.Reason, SR.crmcode from StatusTo ST Inner join  StatusReason SR on  ST.STATUSTOID=SR.statustoid where (ST.towhom ='OPN' OR ST.towhom='MM' OR ST.towhom='UGM' OR ST.towhom='RPR' OR ST.towhom='SSM') and SR.crmcode is not null";

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }
        public DataTable getPendingComplaint(string mncCode, string DateFrom, string DateTo, string TimeFrom, string TimeTo, string ParentOrChild = "")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;


                if (ParentOrChild != "")
                {
                    if (mncCode != "" && mncCode != "0")
                    {
                        _commnadData.CommandText = "Select StatusReason,COUNT(StatusReason) from complaint	 where trim(ComplaintDateTime) !='0'	 AND ComplaintDateTime is not  null AND  (MNCCODE	in(" + mncCode + "))  " +
                            //     "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >= To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI')     AND 	To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI'))  " +
                          " AND " +
                            " (      (STATUS    NOT    IN    ('E0005',    'E0006'))        OR   ( STATUS    IN    ('E0005',    'E0006') AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + DateFrom + "',    'yyyyMMdd') )) " +
                         " ) " +
                         " AND TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')   <       TO_DATE('" + DateFrom + "',    'yyyyMMdd')  AND StatusReason is not null   AND " + ParentOrChild + " group by StatusReason";
                    }
                    else
                    {
                        _commnadData.CommandText = "Select StatusReason,COUNT(StatusReason) from complaint	 where trim(ComplaintDateTime) !='0'	 AND ComplaintDateTime is not  null  " +
                            //     "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >= To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI')     AND 	To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI'))  " +
                            " AND " +
                            " (      (STATUS    NOT    IN    ('E0005',    'E0006'))        OR   ( STATUS    IN    ('E0005',    'E0006') AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + DateFrom + "',    'yyyyMMdd') )) " +
                            " ) " +
                            " AND TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')   <       TO_DATE('" + DateFrom + "',    'yyyyMMdd')  AND StatusReason is not null   AND " + ParentOrChild + " group by StatusReason";
                    }

                }
                else
                {
                    if (mncCode != "" && mncCode != "0")
                    {
                        _commnadData.CommandText = "Select StatusReason,COUNT(StatusReason) from complaint	 where trim(ComplaintDateTime) !='0'	 AND ComplaintDateTime is not  null AND  (MNCCODE	in(" + mncCode + "))  " +
                           "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >= To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI')     AND 	To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI'))  " +
                           " AND " +
                      " (      (STATUS    NOT    IN    ('E0005',    'E0006'))        OR   ( STATUS    IN    ('E0005',    'E0006') AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + DateFrom + "',    'yyyyMMdd') )) " +
                          " ) " +
                          " AND TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')   <       TO_DATE('" + DateFrom + "',    'yyyyMMdd')  AND StatusReason is not null   group by StatusReason";
                    }
                    else
                    {
                        _commnadData.CommandText = "Select StatusReason,COUNT(StatusReason) from complaint	 where trim(ComplaintDateTime) !='0'	 AND ComplaintDateTime is not  null   " +
                        "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >= To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI')     AND 	To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI'))  " +
                        " AND " +
                   " (      (STATUS    NOT    IN    ('E0005',    'E0006'))        OR   ( STATUS    IN    ('E0005',    'E0006') AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + DateFrom + "',    'yyyyMMdd') )) " +
                       " ) " +
                       " AND TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')   <       TO_DATE('" + DateFrom + "',    'yyyyMMdd')  AND StatusReason is not null   group by StatusReason";
                    }

                }

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }




        //public DataTable getForwardPendingComplaint(string mncCode, string DateFrom, string DateTo, string TimeFrom, string TimeTo, string ParentOrChild = "", string QueryString = "")
        //{
        //    //Creating object of DAL class
        //    CommandData _commnadData = new CommandData();

        //    try
        //    {
        //        _commnadData._CommandType = CommandType.Text;
        //        if (ParentOrChild != "")
        //        {
        //            if (mncCode != "" && mncCode != "0")
        //            {
        //                _commnadData.CommandText = "Select StatusTo,COUNT(StatusReason) from complaint	 where trim(ComplaintDateTime) !='0'	 AND ComplaintDateTime is not  null and upper(trim(statusTo)) in ('OPN','RPR','UGM','SSM','MM') AND  (MNCCODE	in(" + mncCode + "))  " +
        //                    // "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >= To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI')     AND 	To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI'))  " +
        //                     " AND " +
        //                " (      (STATUS    NOT    IN    ('E0005',    'E0006'))        OR   ( STATUS    IN    ('E0005',    'E0006') AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + TimeFrom + "',    'yyyyMMddHH24MIss') )) " +
        //                    " ) " +
        //                    " AND TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')   <       TO_DATE('" + TimeFrom + "',    'yyyyMMddHH24MIss')  AND StatusReason is not null and " + ParentOrChild + "   group by StatusTo";
        //            }
        //            else
        //            {
        //                _commnadData.CommandText = "Select StatusTo,COUNT(StatusReason) from complaint	 where trim(ComplaintDateTime) !='0'	 AND ComplaintDateTime is not  null and upper(trim(statusTo)) in ('OPN','RPR','UGM','SSM','MM')  " +
        //                    // "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >= To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI')     AND 	To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI'))  " +
        //                    " AND " +
        //               " (      (STATUS    NOT    IN    ('E0005',    'E0006'))        OR   ( STATUS    IN    ('E0005',    'E0006') AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + TimeFrom + "',    'yyyyMMddHH24MIss') )) " +
        //                   " ) " +
        //                   " AND TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')   <       TO_DATE('" + TimeFrom + "',    'yyyyMMddHH24MIss')  AND StatusReason is not null and " + ParentOrChild + "  group by StatusTo";
        //            }

        //        }
        //        else
        //        {

        //            if (mncCode != "" && mncCode != "0")
        //            {
        //                _commnadData.CommandText = "Select StatusTo,COUNT(StatusTo) from complaint	 where trim(ComplaintDateTime) !='0'	 AND ComplaintDateTime is not  null and upper(trim(statusTo)) in ('OPN','RPR','UGM','SSM','MM') AND  (MNCCODE	in(" + mncCode + "))  " +
        //                    " AND " +
        //               " (    (  (STATUS    NOT    IN    ('E0005',    'E0006'))        OR   ( STATUS    IN    ('E0005',    'E0006') AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + TimeFrom + "',    'yyyyMMddHH24MIss') )) ) " +
        //                    "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >= To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI')     AND 	To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI'))  " +

        //               " ) " +
        //                   " AND TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')   <       TO_DATE('" + TimeFrom + "',    'yyyyMMddHH24MIss')  AND StatusReason is not null   group by StatusTo";
        //            }
        //            else
        //            {
        //                _commnadData.CommandText = "Select StatusTo,COUNT(StatusTo) from complaint	 where trim(ComplaintDateTime) !='0'	 AND ComplaintDateTime is not  null and upper(trim(statusTo)) in ('OPN','RPR','UGM','SSM','MM')   " +
        //                  "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >= To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI')     AND 	To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI'))  " +
        //                  " AND " +
        //             " (      (STATUS    NOT    IN    ('E0005',    'E0006'))        OR   ( STATUS    IN    ('E0005',    'E0006') AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + TimeFrom + "',    'yyyyMMddHH24MIss') )) " +
        //                 " ) " +
        //                 " AND TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')   <       TO_DATE('" + TimeFrom + "',    'yyyyMMddHH24MIss')  AND StatusReason is not null   group by StatusTo";
        //            }

        //        }
        //        _commnadData.OpenWithOutTrans();

        //        //Executing Query
        //        DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

        //        return _ds.Tables[0];
        //    }
        //    catch (Exception ex)
        //    {
        //        //Console.WriteLine("No record found");
        //        throw ex;
        //    }
        //    finally
        //    {
        //        //Console.WriteLine("No ");
        //        _commnadData.Close();

        //    }
        //}


        public DataTable getForwardPendingComplaint(string mncCode, string DateFrom, string DateTo, string TimeFrom, string TimeTo, string ParentOrChild = "")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (ParentOrChild != "")
                {
                    if (mncCode != "" && mncCode != "0")
                    {
                        _commnadData.CommandText =

                       

                            "Select StatusTo,COUNT(StatusReason) from complaint	 where trim(ComplaintDateTime) !='0'	 AND ComplaintDateTime is not  null and upper(trim(statusTo)) in ('OPN','RPR','UGM','SSM','MM','HT') AND  (MNCCODE	in(" + mncCode + "))  " +
                            // "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >= To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI')     AND 	To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI'))  " +
                             " AND " +
                        " (      (STATUS    NOT    IN    ('E0005',    'E0006'))        OR   ( STATUS    IN    ('E0005',    'E0006') AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + DateFrom + "',    'yyyyMMdd') )) " +
                            " ) " +
                            " AND TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')   <       TO_DATE('" + DateFrom + "',    'yyyyMMdd')  AND StatusReason is not null   AND " + ParentOrChild + " group by StatusTo";
                    }
                    else
                    {
                        _commnadData.CommandText = "Select StatusTo,COUNT(StatusReason) from complaint	 where trim(ComplaintDateTime) !='0'	 AND ComplaintDateTime is not  null and upper(trim(statusTo)) in ('OPN','RPR','UGM','SSM','MM','HT')  " +
                            // "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >= To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI')     AND 	To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI'))  " +
                            " AND " +
                       " (      (STATUS    NOT    IN    ('E0005',    'E0006'))        OR   ( STATUS    IN    ('E0005',    'E0006') AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + DateFrom + "',    'yyyyMMdd') )) " +
                           " ) " +
                           " AND TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')   <       TO_DATE('" + DateFrom + "',    'yyyyMMdd')  AND StatusReason is not null   AND " + ParentOrChild + " group by StatusTo";
                    }

                }
                else
                {

                    if (mncCode != "" && mncCode != "0")
                    {
                        _commnadData.CommandText = "Select StatusTo,COUNT(StatusTo) from complaint	 where trim(ComplaintDateTime) !='0'	 AND ComplaintDateTime is not  null and upper(trim(statusTo)) in ('OPN','RPR','UGM','SSM','MM','HT') AND  (MNCCODE	in(" + mncCode + "))  " +
                            "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >= To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI')     AND 	To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI'))  " +
                            " AND " +
                       " (      (STATUS    NOT    IN    ('E0005',    'E0006'))        OR   ( STATUS    IN    ('E0005',    'E0006') AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + DateFrom + "',    'yyyyMMdd') )) " +
                           " ) " +
                           " AND TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')   <       TO_DATE('" + DateFrom + "',    'yyyyMMdd')  AND StatusReason is not null   group by StatusTo";
                    }
                    else
                    {
                        _commnadData.CommandText = "Select StatusTo,COUNT(StatusTo) from complaint	 where trim(ComplaintDateTime) !='0'	 AND ComplaintDateTime is not  null and upper(trim(statusTo)) in ('OPN','RPR','UGM','SSM','MM','HT')   " +
                          "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >= To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI')     AND 	To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI'))  " +
                          " AND " +
                     " (      (STATUS    NOT    IN    ('E0005',    'E0006'))        OR   ( STATUS    IN    ('E0005',    'E0006') AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + DateFrom + "',    'yyyyMMdd') )) " +
                         " ) " +
                         " AND TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')   <       TO_DATE('" + DateFrom + "',    'yyyyMMdd')  AND StatusReason is not null   group by StatusTo";
                    }

                }
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable getStatusReason()
        {
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "Select Reason,CRMCODE from StatusReason";

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        public DataTable GetNewFault(string mncCode, string DateFrom, string DateTo, string TimeFrom, string TimeTo, string ParentOrChild = "")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;


                if (ParentOrChild != "")
                {
                    if (mncCode != "" && mncCode != "0")
                    {
                        _commnadData.CommandText = "select StatusReason,Count(StatusReason) from complaint where trim(ComplaintDateTime) !='0'   " +
                            // "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                            //     "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +

                            " AND ComplaintDateTime is not  null AND (MNCCODE	in(" + mncCode + "))		AND ( (TO_DATE(ComplaintDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss'))) AND  StatusReason is not null AND " + ParentOrChild + "  group by StatusReason ";
                    }
                    else
                    {
                        _commnadData.CommandText = "select StatusReason,Count(StatusReason) from complaint where trim(ComplaintDateTime) !='0'   " +
                            // "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                            //     "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +

                           " AND ComplaintDateTime is not  null 		AND ( (TO_DATE(ComplaintDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss'))) AND  StatusReason is not null AND " + ParentOrChild + "  group by StatusReason ";
                    }

                }
                else
                {
                    if (mncCode != "" && mncCode != "0")
                    {
                        _commnadData.CommandText = "select StatusReason,Count(StatusReason) from complaint where trim(ComplaintDateTime) !='0' " +
                            "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                                      "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24:MI:SS') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24:MI:SS')  )" +

                            " AND ComplaintDateTime is not  null AND (MNCCODE	in(" + mncCode + "))		AND (TO_DATE(ComplaintDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss')) AND StatusReason is not null group by StatusReason";
                    }
                    else
                    {
                        _commnadData.CommandText = "select StatusReason,Count(StatusReason) from complaint where trim(ComplaintDateTime) !='0' " +
                           "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                                     "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24:MI:SS') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24:MI:SS')  )" +

                           " AND ComplaintDateTime is not  null 		AND (TO_DATE(ComplaintDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss')) AND StatusReason is not null group by StatusReason";
                    }
                }

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }



        public DataTable GetForwardNewFault(string mncCode, string DateFrom, string DateTo, string TimeFrom, string TimeTo, string ParentOrChild = "")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (ParentOrChild != "")
                {
                    if (mncCode != "" && mncCode != "0")
                    {
                        _commnadData.CommandText = "select StatusTo,Count(*) from complaint where upper(trim(statusTo)) in ('OPN','RPR','UGM','SSM','MM','HT')  AND trim(ComplaintDateTime) !='0' AND ComplaintDateTime is not  null AND (MNCCODE	in(" + mncCode + "))  " +
                                                   " AND ((TO_DATE(ComplaintDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss'))) AND StatusReason is not null  AND " + ParentOrChild + " group by StatusTo ";
                    }
                    else
                    {
                        _commnadData.CommandText = "select StatusTo,Count(*) from complaint where upper(trim(statusTo)) in ('OPN','RPR','UGM','SSM','MM','HT')  AND trim(ComplaintDateTime) !='0' AND ComplaintDateTime is not  null   " +
                                                 " 	AND ((TO_DATE(ComplaintDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss'))) AND StatusReason is not null  AND " + ParentOrChild + " group by StatusTo ";
                    }

                }
                else
                {
                    if (mncCode != "" && mncCode != "0")
                    {
                        _commnadData.CommandText = "select StatusTo,Count(*) from complaint where upper(trim(statusTo)) in ('OPN','RPR','UGM','SSM','MM','HT')  AND trim(ComplaintDateTime) !='0' AND ComplaintDateTime is not  null AND (MNCCODE	in(" + mncCode + "))  " +
                            "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                            "  AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +
                            "  AND ((TO_DATE(ComplaintDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss'))) AND StatusReason is not null  group by StatusTo  ";
                    }
                    else
                    {
                        _commnadData.CommandText = "select StatusTo,Count(*) from complaint where upper(trim(statusTo)) in ('OPN','RPR','UGM','SSM','MM','HT')  AND trim(ComplaintDateTime) !='0' AND ComplaintDateTime is not  null   " +
                                        "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                        "  AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +
                                        "  AND ((TO_DATE(ComplaintDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss'))) AND StatusReason is not null  group by StatusTo  ";
                    }

                }

                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetFaultRepaired(string mncCode, string DateFrom, string DateTo, string TimeFrom, string TimeTo, string ParentOrChild = "")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;


                if (ParentOrChild != "")
                {
                    if (mncCode != "" && mncCode != "0")
                    {
                        _commnadData.CommandText = "select StatusReason,Count(StatusReason) from complaint where trim(ComplaintDateTime) !='0'  " +
                            //    "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                            // "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +

                            " AND ComplaintDateTime is not  null  AND (MNCCODE	in(" + mncCode + "))		AND (((TO_DATE(ComplaintAttemptDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss') AND Status in('E0006')) OR (TO_DATE(StatusDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss') AND Status in('E0005')))) AND StatusReason is not null AND " + ParentOrChild + "  group by StatusReason";
                    }
                    else
                    {
                        _commnadData.CommandText = "select StatusReason,Count(StatusReason) from complaint where trim(ComplaintDateTime) !='0'  " +
                            //    "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                            // "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +

                                                   " AND ComplaintDateTime is not  null  		AND (((TO_DATE(ComplaintAttemptDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss') AND Status in('E0006')) OR (TO_DATE(StatusDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss') AND Status in('E0005')))) AND StatusReason is not null AND " + ParentOrChild + "  group by StatusReason";
                    }

                }
                else
                {
                    if (mncCode != "" && mncCode != "0")
                    {
                        _commnadData.CommandText = "select StatusReason,Count(StatusReason) from complaint where trim(ComplaintDateTime) !='0'  " +
                            "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                                          "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +

                            " AND ComplaintDateTime is not  null AND (MNCCODE	in(" + mncCode + "))		AND ((TO_DATE(ComplaintAttemptDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss') AND Status in('E0006')) OR (TO_DATE(StatusDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss') AND Status in('E0005'))) AND StatusReason is not null group by StatusReason";
                    }
                    else
                    {
                        _commnadData.CommandText = "select StatusReason,Count(StatusReason) from complaint where trim(ComplaintDateTime) !='0'  " +
                           "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                                         "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +

                           " AND ComplaintDateTime is not  null 		AND ((TO_DATE(ComplaintAttemptDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss') AND Status in('E0006')) OR (TO_DATE(StatusDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss') AND Status in('E0005'))) AND StatusReason is not null group by StatusReason";
                    }

                }

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        public DataTable GetForwardFaultRepaired(string mncCode, string DateFrom, string DateTo, string TimeFrom, string TimeTo, string ParentOrChild = "")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;

                if (ParentOrChild != "")
                {
                    if (mncCode != "" && mncCode != "0")
                    {
                        _commnadData.CommandText = "select StatusTo,Count(*) from complaint where upper(trim(statusTo)) in ('OPN','RPR','UGM','SSM','MM','HT') AND trim(ComplaintDateTime) !='0' AND ComplaintDateTime is not  null AND (MNCCODE	in(" + mncCode + "))   " +
                                        " AND (((TO_DATE(ComplaintAttemptDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss') AND Status in('E0006')) OR (TO_DATE(StatusDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss') AND Status in('E0005')))) AND StatusReason is not null AND " + ParentOrChild + "  group by StatusTo";
                                        

                    }
                    else
                    {
                        _commnadData.CommandText = "select StatusTo,Count(*) from complaint where upper(trim(statusTo)) in ('OPN','RPR','UGM','SSM','MM','HT') AND trim(ComplaintDateTime) !='0' AND ComplaintDateTime is not  null  " +
                         " AND (((TO_DATE(ComplaintAttemptDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss') AND Status in('E0006')) OR (TO_DATE(StatusDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss') AND Status in('E0005')))) AND StatusReason is not null AND " + ParentOrChild + "  group by StatusTo";
                        

                    }

                }
                else
                {
                    if (mncCode != "" && mncCode != "0")
                    {
                        _commnadData.CommandText = "select StatusTo,Count(*) from complaint where upper(trim(statusTo)) in ('OPN','RPR','UGM','SSM','MM','HT') AND trim(ComplaintDateTime) !='0' AND ComplaintDateTime is not  null AND (MNCCODE	in(" + mncCode + ")) " +
                                                    "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                                    "  AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +
                                                    "  AND ((TO_DATE(ComplaintAttemptDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss') AND Status in('E0006')) OR (TO_DATE(StatusDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss') AND Status in('E0005'))) AND StatusReason is not null group by StatusTo ";
                                                   

                    }
                    else
                    {
                        _commnadData.CommandText = "select StatusTo,Count(*) from complaint where upper(trim(statusTo)) in ('OPN','RPR','UGM','SSM','MM','HT') AND trim(ComplaintDateTime) !='0' AND ComplaintDateTime is not  null " +
                           "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                           "  AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +
                           "  AND ((TO_DATE(ComplaintAttemptDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss') AND Status in('E0006')) OR (TO_DATE(StatusDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss') AND Status in('E0005'))) AND StatusReason is not null group by StatusTo ";
                           

                    }
                }

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        #endregion

        //    #region Trafo Report




        //    public DataTable GetTrafoReport(string QueryString, string currentSHiftDateFrom, string currentShiftDateTO, string TimeFrom, string TimeTo,string _parameter)
        //    {
        //        //Creating object of DAL class
        //        CommandData _commnadData = new CommandData();

        //        try
        //        {
        //            _commnadData._CommandType = CommandType.Text;

        //            if (_parameter !="")
        //            {
        //                _commnadData.CommandText = "SELECT *  FROM    (SELECT PMTNAME,   StatusREASON       FROM COMPLAINT C       WHERE  trim(ComplaintDateTime) !='0'  " +
        //             "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" +  TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
        //                     "   AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" +  TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +

        //           " AND ComplaintDateTime is not  null 		AND (TO_DATE(ComplaintDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" +  TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" +   TimeTo + "',	'yyyyMMddHH24MIss')) and  STATUSREASON IN ('LOW VOLTAGE','Feeder Trip','DB to O/H Lead Repaired/Replaced') AND "+_parameter+"     )    PIVOT      (              COUNT(StatusREASON)              FOR StatusREASON IN (" + QueryString + ")   )";              //Adding Parameters
        //            }
        //            else
        //            {
        //                _commnadData.CommandText = "SELECT *  FROM    (SELECT PMTNAME,   StatusREASON       FROM COMPLAINT C       WHERE  trim(ComplaintDateTime) !='0'  " +
        //             "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" +   TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
        //                     "   AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" +  TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +

        //           " AND ComplaintDateTime is not  null 		AND (TO_DATE(ComplaintDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" +  TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss')) and  STATUSREASON IN ('LOW VOLTAGE','Feeder Trip','DB to O/H Lead Repaired/Replaced')      )    PIVOT      (              COUNT(StatusREASON)              FOR StatusREASON IN (" + QueryString + ")   )";              //Adding Parameters
        //            }


        //            // _commnadData.AddParameter("@UserName", userID);

        //            //opening connection
        //            _commnadData.OpenWithOutTrans();

        //            //Executing Query
        //            DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;
        //            return _ds.Tables[0];

        //        }
        //        catch (Exception ex)
        //        {
        //            //Console.WriteLine("No record found");
        //            throw ex;
        //        }
        //        finally
        //        {
        //            //Console.WriteLine("No ");
        //            _commnadData.Close();


        //        }
        //        // return _ds.Tables[0];
        //    }




        //    public DataTable GetTrafoReportWithOutOther(string QueryString, string currentSHiftDateFrom, string currentShiftDateTO, string ParentOrChild)
        //    {
        //        //Creating object of DAL class
        //        CommandData _commnadData = new CommandData();

        //        try
        //        {
        //            _commnadData._CommandType = CommandType.Text;


        //            if (ParentOrChild != "")
        //            {
        //                _commnadData.CommandText = "SELECT *  FROM    (SELECT  SUM('0') Other, PMTNAME,  DTSID,to_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),MM-dd-yyyy HH24:MI:ss') ComplaintDate ,StatusReason       FROM COMPLAINT C       WHERE  trim(ComplaintDateTime) !='0' AND ComplaintDateTime is not  null AND " + ParentOrChild + " 		AND (TO_DATE(ComplaintDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + currentSHiftDateFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + currentShiftDateTO + "',	'yyyyMMddHH24MIss')) and  STATUSREASON IN (" + QueryString + ")      group by  PMTNAME,  DTSID,ComplaintDateTime   ,StatusREASON )    PIVOT      (              COUNT(StatusREASON)              FOR StatusREASON IN (" + QueryString + ")   )";              //Adding Parameters

        //            }
        //            else
        //            {
        //                _commnadData.CommandText = "SELECT *  FROM    (SELECT  SUM('0') Other, PMTNAME,  DTSID,to_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'MM-dd-yyyy HH24:MI:ss') ComplaintDate ,StatusReason       FROM COMPLAINT C       WHERE  trim(ComplaintDateTime) !='0' AND ComplaintDateTime is not  null		AND (TO_DATE(ComplaintDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + currentSHiftDateFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + currentShiftDateTO + "',	'yyyyMMddHH24MIss')) and  STATUSREASON IN (" + QueryString + ")      group by  PMTNAME,  DTSID,ComplaintDateTime   ,StatusREASON )    PIVOT      (              COUNT(StatusREASON)              FOR StatusREASON IN (" + QueryString + ")   )";              //Adding Parameters

        //            }
        //            // _commnadData.AddParameter("@UserName", userID);

        //            //opening connection
        //            _commnadData.OpenWithOutTrans();

        //            //Executing Query
        //            DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

        //            return _ds.Tables[0];
        //        }
        //        catch (Exception ex)
        //        {
        //            //Console.WriteLine("No record found");
        //            throw ex;
        //        }
        //        finally
        //        {
        //            //Console.WriteLine("No ");
        //            _commnadData.Close();

        //        }
        //    }
        //    public DataTable GetTrafoReportWithOther(string QueryString, string currentSHiftDateFrom, string currentShiftDateTO, string ParentOrChild)
        //    {
        //        //Creating object of DAL class
        //        CommandData _commnadData = new CommandData();

        //        try
        //        {
        //            _commnadData._CommandType = CommandType.Text;


        //            if (ParentOrChild != "")
        //            {


        //                _commnadData.CommandText = " SELECT * from( select  count(StatusReason)  Other ,PMTNAME,DTSID,ComplaintDate from( " +

        //    " (SELECT PMTNAME,  DTSID,to_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'MM-dd-yyyy HH24:MI:ss') ComplaintDate ,StatusREASON ,  " +
        //            "  StatusREASON test    FROM COMPLAINT C        " +
        //          "  WHERE  trim(ComplaintDateTime) !='0' AND ComplaintDateTime is not  null  AND " + ParentOrChild + "    " +
        //            "   AND (TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss')  " +
        //                "    BETWEEN        TO_DATE('" + currentSHiftDateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + currentShiftDateTO + "',    'yyyyMMddHH24MIss' " +
        //                "    )) " +
        //                "  and  STATUSREASON  NOT IN (" + QueryString + ")   " +
        //                "   group by  PMTNAME,  DTSID,ComplaintDateTime,StatusREASON     " +
        //                 "    ) " +
        //     "  PIVOT    " +
        //     "  (    COUNT(test)    FOR test IN (" + QueryString + ") " +

        //" ) ) Group By PMTNAME,  DTSID,ComplaintDate,StatusREASON)";

        //            }
        //            else
        //            {
        //                _commnadData.CommandText = " SELECT * from( select  count(StatusReason)  Other ,PMTNAME,DTSID,ComplaintDate from( " +

        //     " (SELECT PMTNAME,  DTSID,to_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'MM-dd-yyyy HH24:MI:ss') ComplaintDate ,StatusREASON ,  " +
        //             "  StatusREASON test    FROM COMPLAINT C        " +
        //           "  WHERE  trim(ComplaintDateTime) !='0' AND ComplaintDateTime is not  null     " +
        //             "   AND (TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss')  " +
        //                 "    BETWEEN        TO_DATE('" + currentSHiftDateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + currentShiftDateTO + "',    'yyyyMMddHH24MIss' " +
        //                 "    )) " +
        //                 "  and  STATUSREASON  NOT IN (" + QueryString + ")   " +
        //                 "   group by  PMTNAME,  DTSID,ComplaintDateTime,StatusREASON     " +
        //                  "    ) " +
        //      "  PIVOT    " +
        //      "  (    COUNT(test)    FOR test IN (" + QueryString + ") " +

        // "  ) ) Group By PMTNAME,  DTSID,ComplaintDate,StatusREASON ) ";
        //            }
        //            // _commnadData.AddParameter("@UserName", userID);

        //            //opening connection
        //            _commnadData.OpenWithOutTrans();

        //            //Executing Query
        //            DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

        //            return _ds.Tables[0];
        //        }
        //        catch (Exception ex)
        //        {
        //            //Console.WriteLine("No record found");
        //            throw ex;
        //        }
        //        finally
        //        {
        //            //Console.WriteLine("No ");
        //            _commnadData.Close();

        //        }
        //    }

        //    #endregion



        #region Trafo Report




        public DataTable GetTrafoReport(string QueryString, string currentSHiftDateFrom, string currentShiftDateTO, string TimeFrom, string TimeTo, string _parameter)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;

                if (_parameter != "")
                {
               //     _commnadData.CommandText = "SELECT *  FROM    (SELECT PMTNAME,   Upper(Trim(StatusREASON))  StatusREASON       FROM COMPLAINT C       WHERE  trim(ComplaintDateTime) !='0' and PMTNAME is not null  " +
               //         // "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
               //         // "   AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +

               //" AND ComplaintDateTime is not  null 		AND (TO_DATE(ComplaintDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss')) AND Trim(statusreason) is not null    AND " + _parameter + "     )    PIVOT      (              COUNT(StatusREASON)              FOR StatusREASON IN (" + QueryString + ")   )";              //Adding Parameters
                    _commnadData.CommandText = "SELECT PMTNAME,   Upper(Trim(StatusREASON))  StatusREASON  ,Upper(Trim(StatusREASON))  Reason     FROM COMPLAINT C       WHERE  trim(ComplaintDateTime) !='0' and PMTNAME is not null  " +
                  " AND ComplaintDateTime is not  null 		AND (TO_DATE(ComplaintDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss')) AND Trim(statusreason) is not null and  Upper(Trim(StatusREASON)) in (" + QueryString + ") AND " + _parameter ;              //Adding Parameters
                }
                else
                {
               //     _commnadData.CommandText = "SELECT *  FROM    (SELECT PMTNAME,   Upper(Trim(StatusREASON))  StatusREASON       FROM COMPLAINT C       WHERE  trim(ComplaintDateTime) !='0' and PMTNAME is not null  " +
               //  "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
               //          "   AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +

               //" AND ComplaintDateTime is not  null 		AND (TO_DATE(ComplaintDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss')) AND Trim(statusreason) is not null      )    PIVOT      (              COUNT(StatusREASON)              FOR StatusREASON IN (" + QueryString + ")   )";              //Adding Parameters


                    _commnadData.CommandText = "SELECT PMTNAME,   Upper(Trim(StatusREASON))  StatusREASON   , Upper(Trim(StatusREASON))  Reason   FROM COMPLAINT C       WHERE  trim(ComplaintDateTime) !='0' and PMTNAME is not null  " +
            "  AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                    "   AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +

          " AND ComplaintDateTime is not  null 		AND (TO_DATE(ComplaintDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + TimeFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + TimeTo + "',	'yyyyMMddHH24MIss')) AND Trim(statusreason) is not null and  Upper(Trim(StatusREASON)) in (" + QueryString + ")    ";              //Adding Parameters
                }


                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;
                return _ds.Tables[0];

            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();


            }
            // return _ds.Tables[0];
        }




        public DataTable GetTrafoReportWithOutOther(string QueryString, string currentSHiftDateFrom, string currentShiftDateTO, string ParentOrChild)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;


                if (ParentOrChild != "")
                {
                    _commnadData.CommandText = "SELECT *  FROM    (SELECT  SUM('0') Other, PMTNAME,  DTSID,to_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),MM-dd-yyyy HH24:MI:ss') ComplaintDate ,StatusReason       FROM COMPLAINT C       WHERE  trim(ComplaintDateTime) !='0' AND ComplaintDateTime is not  null AND " + ParentOrChild + " 		AND (TO_DATE(ComplaintDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + currentSHiftDateFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + currentShiftDateTO + "',	'yyyyMMddHH24MIss')) and  STATUSREASON IN (" + QueryString + ")      group by  PMTNAME,  DTSID,ComplaintDateTime   ,StatusREASON )    PIVOT      (              COUNT(StatusREASON)              FOR StatusREASON IN (" + QueryString + ")   )";              //Adding Parameters

                }
                else
                {
                    _commnadData.CommandText = "SELECT *  FROM    (SELECT  SUM('0') Other, PMTNAME,  DTSID,to_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'MM-dd-yyyy HH24:MI:ss') ComplaintDate ,StatusReason       FROM COMPLAINT C       WHERE  trim(ComplaintDateTime) !='0' AND ComplaintDateTime is not  null		AND (TO_DATE(ComplaintDateTime,	'yyyyMMddHH24MIss')	BETWEEN		TO_DATE('" + currentSHiftDateFrom + "',	'yyyyMMddHH24MIss')	AND		TO_DATE('" + currentShiftDateTO + "',	'yyyyMMddHH24MIss')) and  STATUSREASON IN (" + QueryString + ")      group by  PMTNAME,  DTSID,ComplaintDateTime   ,StatusREASON )    PIVOT      (              COUNT(StatusREASON)              FOR StatusREASON IN (" + QueryString + ")   )";              //Adding Parameters

                }
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }
        public DataTable GetTrafoReportWithOther(string QueryString, string currentSHiftDateFrom, string currentShiftDateTO, string ParentOrChild)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;


                if (ParentOrChild != "")
                {


                    _commnadData.CommandText = " SELECT * from( select  count(StatusReason)  Other ,PMTNAME,DTSID,ComplaintDate from( " +

        " (SELECT PMTNAME,  DTSID,to_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'MM-dd-yyyy HH24:MI:ss') ComplaintDate ,StatusREASON ,  " +
                "  StatusREASON test    FROM COMPLAINT C        " +
              "  WHERE  trim(ComplaintDateTime) !='0' AND ComplaintDateTime is not  null  AND " + ParentOrChild + "    " +
                "   AND (TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss')  " +
                    "    BETWEEN        TO_DATE('" + currentSHiftDateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + currentShiftDateTO + "',    'yyyyMMddHH24MIss' " +
                    "    )) " +
                    "  and  STATUSREASON  NOT IN (" + QueryString + ")   " +
                    "   group by  PMTNAME,  DTSID,ComplaintDateTime,StatusREASON     " +
                     "    ) " +
         "  PIVOT    " +
         "  (    COUNT(test)    FOR test IN (" + QueryString + ") " +

    " ) ) Group By PMTNAME,  DTSID,ComplaintDate,StatusREASON)";

                }
                else
                {
                    _commnadData.CommandText = " SELECT * from( select  count(StatusReason)  Other ,PMTNAME,DTSID,ComplaintDate from( " +

         " (SELECT PMTNAME,  DTSID,to_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'MM-dd-yyyy HH24:MI:ss') ComplaintDate ,StatusREASON ,  " +
                 "  StatusREASON test    FROM COMPLAINT C        " +
               "  WHERE  trim(ComplaintDateTime) !='0' AND ComplaintDateTime is not  null     " +
                 "   AND (TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss')  " +
                     "    BETWEEN        TO_DATE('" + currentSHiftDateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + currentShiftDateTO + "',    'yyyyMMddHH24MIss' " +
                     "    )) " +
                     "  and  STATUSREASON  NOT IN (" + QueryString + ")   " +
                     "   group by  PMTNAME,  DTSID,ComplaintDateTime,StatusREASON     " +
                      "    ) " +
          "  PIVOT    " +
          "  (    COUNT(test)    FOR test IN (" + QueryString + ") " +

     "  ) ) Group By PMTNAME,  DTSID,ComplaintDate,StatusREASON ) ";
                }
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        #endregion

        //#region Media Cell Report



        //public DataTable GetMediaCellRecord(string dateFrom, string dateTo)
        //{
        //    //Creating object of DAL class
        //    CommandData _commnadData = new CommandData();

        //    try
        //    {
        //        _commnadData._CommandType = CommandType.Text;
        //        _commnadData.CommandText = "select SUBSTR(ComplaintDateTime,7,2) as ComplaintDate, SUBSTR(ComplaintDateTime,9,2) ||':'|| SUBSTR(ComplaintDateTime,11,2) RecTime  ,CrmserviceTicketNo,Address, InternalNode ,WIRESIZE,WIREREQUIRED,DTSID from complaint where trim(COMPLAINTDATETIME) !='0' AND COMPLAINTDATETIME is not null AND TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    >=    TO_DATE('" + dateFrom + "',    'yyyyMMddHH24MIss') AND TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <=    TO_DATE('" + dateTo + "',    'yyyyMMddHH24MIss')";

        //        //Adding Parameters
        //        // _commnadData.AddParameter("@UserName", userID);

        //        //opening connection
        //        _commnadData.OpenWithOutTrans();

        //        //Executing Query
        //        DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

        //        return _ds.Tables[0];
        //    }
        //    catch (Exception ex)
        //    {
        //        //Console.WriteLine("No record found");
        //        throw ex;
        //    }
        //    finally
        //    {
        //        //Console.WriteLine("No ");
        //        _commnadData.Close();

        //    }
        //}

        //#endregion

        #region Media Cell Report



        public DataTable GetMediaCellRecord(string dateFrom, string dateTo, string querystring)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (querystring != "")
                {
                    _commnadData.CommandText = "select SUBSTR(ComplaintDateTime,7,2) as ComplaintDate, SUBSTR(ComplaintDateTime,9,2) ||':'|| SUBSTR(ComplaintDateTime,11,2) RecTime  ,CrmserviceTicketNo,Address, InternalNode ,WIRESIZE,WIREREQUIRED,DTSID from complaint where trim(COMPLAINTDATETIME) !='0' AND COMPLAINTDATETIME is not null AND TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    >=    TO_DATE('" + dateFrom + "',    'yyyyMMddHH24MIss') AND TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <=    TO_DATE('" + dateTo + "',    'yyyyMMddHH24MIss')  ANd " + querystring + " order by  ComplaintDateTime ASC ";

                }
                else
                {
                    _commnadData.CommandText = "select SUBSTR(ComplaintDateTime,7,2) as ComplaintDate, SUBSTR(ComplaintDateTime,9,2) ||':'|| SUBSTR(ComplaintDateTime,11,2) RecTime  ,CrmserviceTicketNo,Address, InternalNode ,WIRESIZE,WIREREQUIRED,DTSID from complaint where trim(COMPLAINTDATETIME) !='0' AND COMPLAINTDATETIME is not null AND TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    >=    TO_DATE('" + dateFrom + "',    'yyyyMMddHH24MIss') AND TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <=    TO_DATE('" + dateTo + "',    'yyyyMMddHH24MIss') order by  ComplaintDateTime ASC  ";
                }
                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        #endregion

        #region Log Report

        public DataTable GetComplaintDetailByCrmNo(string CRMSERVICETICKETNO = "")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "Select * from Complaint Where CRMSERVICETICKETNO='" + CRMSERVICETICKETNO + "'";

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }



        public DataTable GetFirstCompleteComplaintLogDetailByCrmNo(string CRMSERVICETICKETNO = "")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "select MIN(Complaint_Log_ID) from COmplaints_log  where status='E0005' AND Crmserviceticket_no='" + CRMSERVICETICKETNO + "' ";


                _commnadData.OpenWithOutTrans();


                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetFirstCompleteComplaintLogDetailByCrmNo(string CRMSERVICETICKETNO = "", Int64 ComplaintLogId = 0)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "select To_Date(Complaint_DateTime,'yyyyMMddHH24MIss') Complaint_DateTime,C.* from COmplaints_log C  where  Crmserviceticket_no='" + CRMSERVICETICKETNO + "' and COMPLAINT_LOG_ID <= " + ComplaintLogId + " ORder by  COMPLAINT_LOG_ID ASC";


                _commnadData.OpenWithOutTrans();


                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }



        public DataTable GetFirstReOpenComplaintId(string CRMSERVICETICKETNO = "", string DateFrom = "")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "select MIN(Complaint_Log_ID) from COmplaints_log C   where status='E0003' AND Crmserviceticket_no='" + CRMSERVICETICKETNO + "' AND To_Date(Status_DateTime,'yyyyMMddHH24MIss') > To_Date('" + DateFrom + "','yyyyMMddHH24MIss') ";


                _commnadData.OpenWithOutTrans();


                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }
        public DataTable GetFirstReOpenCompleteComplaintId(string CRMSERVICETICKETNO = "", Int64 ComplaintLogId = 0)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "select To_Date(C.Complaint_DateTime,'yyyyMMddHH24MIss') Complaint_DateTime, C.* from COmplaints_log C  where    C.Crmserviceticket_no='" + CRMSERVICETICKETNO + "' AND C.COMPLAINT_LOG_ID >= " + ComplaintLogId + " ORder by  C.COMPLAINT_LOG_ID ASC";


                _commnadData.OpenWithOutTrans();


                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }






        public DataTable GetSecondReOpenComplaintId(string CRMSERVICETICKETNO = "", string DateFrom = "")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "select MIN(Complaint_Log_ID) from COmplaints_log C   where status='E0003' AND Crmserviceticket_no='" + CRMSERVICETICKETNO + "' AND To_Date(Status_DateTime,'yyyyMMddHH24MIss') > To_Date('" + DateFrom + "','yyyyMMddHH24MIss') ";


                _commnadData.OpenWithOutTrans();


                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        public DataTable GetSecondReOpenCompleteComplaintId(string CRMSERVICETICKETNO = "", Int64 ComplaintLogId = 0)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "select To_Date(C.Complaint_DateTime,'yyyyMMddHH24MIss') Complaint_DateTime, C.* from COmplaints_log C  where    C.Crmserviceticket_no='" + CRMSERVICETICKETNO + "' AND C.COMPLAINT_LOG_ID >= " + ComplaintLogId + " ORder by  C.COMPLAINT_LOG_ID ASC";


                _commnadData.OpenWithOutTrans();


                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }




        public DataTable GetLastActivityOtherThanComplete(string CRMSERVICETICKETNO = "")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "select MAX(Complaint_Log_ID) from COmplaints_log  where Trim(Action_By) in ('MW','MTL','SAP') AND  Crmserviceticket_no='" + CRMSERVICETICKETNO + "' ";


                _commnadData.OpenWithOutTrans();


                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetComplaintBetweenAssignAndComplete1stAttempt(string LineManName = "", String MTL = "", String DateFrom = "", String DateTo = "")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "select  distinct Crmserviceticket_No,To_Date(Complaint_DateTime,'yyyyMMddHH24MIss') Complaint_DateTime,Status_DateTime Status_DateTime,S.StatusName Status,Action_By,U.UserName  from COmplaints_log C  left join Users U on C.Modified_By=U.UserId inner join Status S on C.Status=S.StatusCode where  To_Date(Status_DateTime,'yyyyMMddHH24MIss') >=To_Date('" + DateFrom + "','yyyyMMddHH24MIss')  AND To_Date(Status_DateTime,'yyyyMMddHH24MIss') <=To_Date('" + DateTo + "','yyyyMMddHH24MIss') AND  Trim(LineMan_Name)='" + LineManName + "' AND MTL='" + MTL + "' ORder by  Status_DateTime ASC";


                _commnadData.OpenWithOutTrans();


                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }



        public DataTable GetMTLOfShift(string MNCCODE = "", string ShiftName = "", string ShiftStartTime = "", string DateFrom = "")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "select Z.ZONEName,Dr.MTL,DR.GANGNAME,D.ShiftSignInTime from DutyRooster D  " +
                                           " inner join DutyRoosterTeam DR on D.DutyRoosterID=DR.DutyRoosterid  inner join Zone Z on Dr.ZoneID=Z.ZoneId" +
                                            "  inner join Centre C on D.centre=C.MNCNAME " +

                                           " Where To_Date(ShiftDate,'DD-MON-YY')=To_Date('" + DateFrom + "','DD-MON-YY') And " +
                                            "  Trim(ShiftName)='" + ShiftName + "' and ShiftStartTime='" + ShiftStartTime + "' and MNCCODE='" + MNCCODE + "'";


                _commnadData.OpenWithOutTrans();


                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }



        public DataTable getComplaintOtherThanReopenAndClose(string MNCCODE = "", string ShiftStartTime = "", string DateTo = "")
        {

            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                //_commnadData.CommandText = "SELECT      DISTINCT    LINEMAN_NAME,    COUNT(DISTINCT  CRMSERVICETICKET_NO)    TotalTicketsAssigned " +

                //                            " FROM        Complaints_LOG "+
                //                            " WHERE       LINEMAN_NAME IS  NOT NULL        AND            MNCCODE =   '"+MNCCODE+"'    AND "+
                //                            " Status  NOT IN  ('E0003',   'E0006')    AND "+
                //                            " TO_DATE(STATUS_DATETIME,    'yyyyMMddHH24MIss')     BETWEEN     "+
                //                            "  TO_DATE('"+ShiftStartTime+"',    'yyyyMMddHH24MIss')      AND     TO_DATE('"+DateTo+"',    'yyyyMMddHH24MIss') "+
                //                            " GROUP   BY  LINEMAN_NAME" ;
                _commnadData.CommandText = " Select    Distinct   LineMan_Name, Count(DISTINCT CRMSERVICETICKET_NO ) TotalTicketsAssigned     FROM     Complaints_LOG " +
                                            "  where Complaint_Log_ID in ( " +
                                            "  select Complaint_Log_id from( " +

                                                                           "   Select     DISTINCT  Max(Complaint_Log_ID)  Complaint_Log_ID, " +
                                                                              " CRMSERVICETICKET_NO      FROM     " +
                                                                             "  Complaints_LOG    " +
                                                                              "  WHERE       LINEMAN_NAME IS  NOT NULL        AND            MNCCODE =   '" + MNCCODE + "'    AND  Status  NOT IN  ('E0003',   'E0006')   " +
                                                                                  "   AND  TO_DATE(STATUS_DATETIME,    'yyyyMMddHH24MIss')     BETWEEN       TO_DATE('" + ShiftStartTime + "',    'yyyyMMddHH24MIss')  " +
                                                                                    "      AND     TO_DATE('" + DateTo + "',    'yyyyMMddHH24MIss') GROUP   BY  CRMSERVICETICKET_NO " +
                                                                            "  ) t " +
                                           "   ) " +
                                           "  AND  LINEMAN_NAME IS  NOT NULL        AND            MNCCODE =   '" + MNCCODE + "'   " +
                                          " AND     Status  NOT IN  ('E0003',   'E0006')    AND  TO_DATE(STATUS_DATETIME,    'yyyyMMddHH24MIss')    " +
                                           "  BETWEEN       TO_DATE('" + ShiftStartTime + "',    'yyyyMMddHH24MIss')      AND     " +
                                            "  TO_DATE('" + DateTo + "',    'yyyyMMddHH24MIss')  GROUP   BY  LINEMAN_NAME";


                _commnadData.OpenWithOutTrans();


                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable getCompleteComplaint(string MNCCODE = "", string ShiftStartTime = "", string DateTo = "")
        {

            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "SELECT      DISTINCT    LINEMAN_NAME,    COUNT(DISTINCT  CRMSERVICETICKET_NO)    TotalTicketsAssigned " +

                                            " FROM        Complaints_LOG " +
                                            " WHERE       LINEMAN_NAME IS  NOT NULL        AND            MNCCODE =   '" + MNCCODE + "'    AND " +
                                            " Status   IN  ('E0005')    AND " +
                                            " TO_DATE(STATUS_DATETIME,    'yyyyMMddHH24MIss')     BETWEEN     " +
                                            "  TO_DATE('" + ShiftStartTime + "',    'yyyyMMddHH24MIss')      AND     TO_DATE('" + DateTo + "',    'yyyyMMddHH24MIss') " +
                                            " GROUP   BY  LINEMAN_NAME";


                _commnadData.OpenWithOutTrans();


                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        #endregion
        public DataTable GetHtOutageReport(string dateFrom, string dateTo, string QueryString,string IBCCode)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    if (IBCCode != "" && IBCCode != null)
                    {
                        _commnadData.CommandText = "select * from (select 	Distinct dts.FEEDERNAME, ord.OUTAGE   from OUTAGERECORD   ord  inner join centre C on ord.MNCNAME=C.MncName  and c.MNCCODE in (" + IBCCode + ") LEFT JOIN DTSDETAILS dts ON dts.FEEDERID = ord.FEEDERID   where   dts.FEEDERNAME IS NOT NULL AND To_Date(To_Char(DateTime,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') between To_Date('" + dateFrom + "','yyyyMMddHH24MIss') AND  To_Date('" + dateTo + "','yyyyMMddHH24MIss')  AND Trim(Outage) in  ('FAULT','SHUTDOWN') AND " + QueryString + "   Group BY ord.FEEDERID,ord.OUTAGE, dts.FEEDERNAME ) PIVOT ( COUNT(Outage)  FOR Outage IN ('FAULT' AS FAULT,'SHUTDOWN' AS SHUTDOWN ))";
                    }
                    else
                    {
                        _commnadData.CommandText = "select * from (select 	Distinct dts.FEEDERNAME, ord.OUTAGE   from OUTAGERECORD  ord LEFT JOIN DTSDETAILS dts ON dts.FEEDERID = ord.FEEDERID   where   dts.FEEDERNAME IS NOT NULL AND To_Date(To_Char(DateTime,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') between To_Date('" + dateFrom + "','yyyyMMddHH24MIss') AND  To_Date('" + dateTo + "','yyyyMMddHH24MIss')  AND Trim(Outage) in  ('FAULT','SHUTDOWN') AND " + QueryString + " Group BY ord.FEEDERID,ord.OUTAGE, dts.FEEDERNAME ) PIVOT ( COUNT(Outage)  FOR Outage IN ('FAULT' AS FAULT,'SHUTDOWN' AS SHUTDOWN ))";

                    }
                }
                else
                {
                    if (IBCCode != "" && IBCCode != null)
                    {
                        _commnadData.CommandText = "select * from (select 	Distinct dts.FEEDERNAME, ord.OUTAGE   from OUTAGERECORD ord inner join centre C on ord.MNCNAME=C.MncName  and c.MNCCODE in (" + IBCCode + ") LEFT JOIN DTSDETAILS dts ON dts.FEEDERID = ord.FEEDERID   where   dts.FEEDERNAME IS NOT NULL AND To_Date(To_Char(DateTime,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') between To_Date('" + dateFrom + "','yyyyMMddHH24MIss') AND  To_Date('" + dateTo + "','yyyyMMddHH24MIss')  AND Trim(Outage) in  ('FAULT','SHUTDOWN') Group BY ord.FEEDERID,ord.OUTAGE, dts.FEEDERNAME ) PIVOT ( COUNT(Outage)  FOR Outage IN ('FAULT' AS FAULT,'SHUTDOWN' AS SHUTDOWN ))";
 
                    }
                    else
                    {
                        _commnadData.CommandText = "select * from (select 	Distinct dts.FEEDERNAME, ord.OUTAGE   from OUTAGERECORD ord LEFT JOIN DTSDETAILS dts ON dts.FEEDERID = ord.FEEDERID   where   dts.FEEDERNAME IS NOT NULL AND To_Date(To_Char(DateTime,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') between To_Date('" + dateFrom + "','yyyyMMddHH24MIss') AND  To_Date('" + dateTo + "','yyyyMMddHH24MIss')  AND Trim(Outage) in  ('FAULT','SHUTDOWN') Group BY ord.FEEDERID,ord.OUTAGE, dts.FEEDERNAME ) PIVOT ( COUNT(Outage)  FOR Outage IN ('FAULT' AS FAULT,'SHUTDOWN' AS SHUTDOWN ))";
                    }
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        //        #region HourlySummaryReport
        //        public DataTable GetCRMTicketsSummaryReport(string ibcCode, string _dateFrom, string _dateTo)
        //        {
        //            //Creating object of DAL class
        //            CommandData _commnadData = new CommandData();

        //            try
        //            {
        //                _commnadData._CommandType = CommandType.Text;

        //                _commnadData.CommandText = "SELECT  ( " +
        //" Select COUNT(*)     PreviousBalance        FROM    COMPLAINT    " +
        //" where (IBCCODE  =   '" + ibcCode + "')    AND               " +
        //" ((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND    " +
        //" ( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          ((STATUS    IN    ('E0005',    'E0006'))        AND     " +
        //" (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND   " +
        //" TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )          OR ( (STATUS    IN    ('E0005',    'E0006'))        AND  " +
        //" (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )   PB,  " +

        //" (Select COUNT(*)     NEWRECEIVED        FROM    COMPLAINT  " +
        //" where (IBCCODE  =   '" + ibcCode + "')    AND       " +
        //" (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND  " +
        //" TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))  NR,         " +

        //" (Select COUNT(*)     RESOLVED               FROM    COMPLAINT    " +
        //" where (IBCCODE  =   '" + ibcCode + "')    AND   " +
        //" (STATUS IN  ('E0005',   'E0006'))   AND     " +
        //" (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND " +
        //" TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))  RESOLVED,           " +

        //" (SELECT                 (Select COUNT(*)     PreviousBalance            FROM    COMPLAINT         " +
        //" where (IBCCODE  =   '" + ibcCode + "')    AND           " +
        //" ((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND" +
        //" ( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          ((STATUS    IN    ('E0005',    'E0006'))        AND  " +
        //" (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND  " +
        //" TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )          OR ( (STATUS    IN    ('E0005',    'E0006'))            AND  " +
        //" (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) ) " +
        //"  +      " +
        // " (Select COUNT(*)     NEWRECEIVED            FROM    COMPLAINT  " +
        // " where (IBCCODE  =   '" + ibcCode + "')    AND               " +
        // " (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND  " +
        //"  TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      TOTAL           FROM    dual)   TOTAL,    " +

        //" (SELECT      COUNT(*) " +
        //" FROM        COMPLAINT " +
        //" WHERE       IBCCODE =   '" + ibcCode + "'           AND " +
        //           "  (" +
        //               "  (STATUS IN  ('E0005',   'E0006')    AND      " +
        //               "  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) " +

        //               "  OR" +

        //               "  (STATUS NOT IN  ('E0005',   'E0006') " +
        //                " )" +

        //          "   )   AND" +
        //          "   (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <   " +
        //            " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))   PENDING,   " +

        //" (SELECT      COUNT(*)" +
        //" FROM        COMPLAINT" +
        //" WHERE       IBCCODE =   '" + ibcCode + "'           AND" +
        //           "  (" +
        //             "    (STATUS IN  ('E0005',   'E0006')    AND " +
        //              "   (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))" +

        //              "   OR" +

        //               "  (STATUS NOT IN  ('E0005',   'E0006') " +
        //               "  )" +

        //           "  )   AND" +
        //           "  (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))    " +
        //           "  AND" +
        //           "  ((round(to_number(TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')  -   TO_DATE(ComplaintDateTime,  'yyyyMMddHH24MIss')) * 24)    >=  8)))      PENDING08HRS, " +


        //" (SELECT      Count(*)" +
        //" FROM        COMPLAINT " +
        //" WHERE       IBCCODE =   '" + ibcCode + "'           AND " +
        //         "    ( " +
        //               "  (STATUS IN  ('E0005',   'E0006')    AND   " +
        //               "  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) " +

        //                " OR " +

        //                " (STATUS NOT IN  ('E0005',   'E0006')  " +
        //               "  ) " +

        //            " )   AND " +
        //           "  (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))    " +
        //            " AND " +
        //           "  (STATUSTO   =   'OPN'))       PENDINGHT, " +

        //" (SELECT      Count(*) " +
        //" FROM        COMPLAINT " +
        //" WHERE       IBCCODE =   '" + ibcCode + "'           AND " +
        //            " ( " +
        //              "   (STATUS IN  ('E0005',   'E0006')    AND  " +
        //               "  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) " +

        //                " OR " +

        //               "  (STATUS NOT IN  ('E0005',   'E0006') " +
        //                " ) " +

        //           "  )   AND " +
        //            " (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))    " +
        //            " AND" +
        //            " ((TRIM(STATUSTO)   !=   'OPN'    OR  TRIM(STATUSTO)    =   ''  OR  STATUSTO    IS  NULL)))   PENDINGLT               FROM    DUAL";

        //                //Adding Parameters
        //                // _commnadData.AddParameter("@UserName", userID);

        //                //opening connection
        //                _commnadData.OpenWithOutTrans();

        //                //Executing Query
        //                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

        //                return _ds.Tables[0];
        //            }
        //            catch (Exception ex)
        //            {
        //                //Console.WriteLine("No record found");
        //                throw ex;
        //            }
        //            finally
        //            {
        //                //Console.WriteLine("No ");
        //                _commnadData.Close();

        //            }
        //        }

        //        public DataTable GetLTHTWireAndLTCableFaultReport(string ibcCode, string _dateFrom, string _dateTo)
        //        {
        //            //Creating object of DAL class
        //            CommandData _commnadData = new CommandData();

        //            try
        //            {
        //                _commnadData._CommandType = CommandType.Text;

        //                _commnadData.CommandText = "SELECT      " +

        //"(Select COUNT(*)     PreviousBalanceLTWIRE          " +
        //"FROM    COMPLAINT               " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND           " +
        //"((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND       " +
        //"((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR             " +
        //"           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')         " +
        //"        ))              AND         " +
        //"( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          " +
        //"((STATUS    IN    ('E0005',    'E0006'))        AND             " +
        //"(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )          " +
        //"OR ( (STATUS    IN    ('E0005',    'E0006'))        " +
        //"AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ))    PreviousBalanceLTWIRE,      " +

        //"(Select COUNT(*)     NEWRECEIVEDLTWIRE          " +
        //"FROM    COMPLAINT           " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND           " +
        //"((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR             " +
        //"           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')     " +
        //"        ))          AND         " +
        //"(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
        //"    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          NEWRECEIVEDLTWIRE,      " +

        //"(SELECT     " +

        //"(Select COUNT(*)     PreviousBalance        " +
        //"FROM    COMPLAINT       " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND       " +
        //"((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND       " +
        //"((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR         " +
        //"           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')         " +
        //"        ))              AND     " +
        //"( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR      " +
        //"((STATUS    IN    ('E0005',    'E0006'))        AND             " +
        //"(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )    " +
        //"OR ( (STATUS    IN    ('E0005',    'E0006'))    " +
        //"AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )       " +

        //"+           " +

        //"(Select COUNT(*)     NEWRECEIVED        " +
        //"FROM    COMPLAINT       " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND           " +
        //"((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR             " +
        //"           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')                                                                     " +
        //"        ))          AND         " +
        //"(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
        //"    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      TOTALLTWIRE     " +

        //"FROM    DUAL)       TOTALLTWIRE,        " +

        //"(Select COUNT(*)     RESOLVEDLTWIRE         " +
        //"FROM    COMPLAINT           " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND   (STATUS IN  ('E0005',   'E0006'))   AND                                                                     " +
        //"((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR " +
        //"           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')" +
        //"        ))          AND         " +
        //"(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND         " +
        //"    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      RESOLVEDLTWIRE,         " +

        //"(SELECT             " +

        //"(Select COUNT(*)     PreviousBalance        " +
        //"FROM    COMPLAINT           " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND           " +
        //"((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND           " +
        //"((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR                 " +
        //"           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')         " +
        //"        ))              AND             " +
        //"( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          " +
        //"((STATUS    IN    ('E0005',    'E0006'))        AND                 " +
        //"(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )          " +
        //"OR ( (STATUS    IN    ('E0005',    'E0006'))            " +
        //"AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )           " +

        //"+               " +

        //"(Select COUNT(*)     NEWRECEIVED            " +
        //"FROM    COMPLAINT                   " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND           " +
        //"((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR             " +
        //"           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')         " +
        //"        ))          AND                 " +
        //"(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
        //"    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          " +

        //"-      " +

        //"(Select COUNT(*)     RESOLVED          " +
        //"FROM    COMPLAINT              " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND   (STATUS IN  ('E0005',   'E0006'))   AND            " +
        //"((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR        " +

        //"    (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')       " +
        // "       ))          AND            " +
        //"(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
        //"    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      PENDINGLTWIRE          " +

        //"FROM    DUAL)       PENDINGLTWIRE,         " +



        //"(Select COUNT(*)     PreviousBalanceHTWIRE         " +
        //"FROM    COMPLAINT              " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND          " +
        //"((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND          " +
        //"((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'         " +
        //"        ))              AND            " +
        //"( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR         " +
        //"((STATUS    IN    ('E0005',    'E0006'))        AND        " +
        //"(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )         " +
        //"OR ( (STATUS    IN    ('E0005',    'E0006'))           " +
        //"AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ))    PreviousBalanceHTWIRE,     " +

        //"(Select COUNT(*)     NEWRECEIVEDHTWIRE         " +
        //"FROM    COMPLAINT          " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND          " +
        //"((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'         " +
        //"        ))          AND            " +
        //"(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND               " +
        //"    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          NEWRECEIVEDHTWIRE,         " +

        //"(SELECT            " +

        //"(Select COUNT(*)     PreviousBalance               " +
        //"FROM    COMPLAINT              " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND      " +
        //"((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND      " +
        //"((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'         " +
        //"        ))              AND                " +
        //"( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR             " +
        //"((STATUS    IN    ('E0005',    'E0006'))        AND        " +
        //"(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )         " +
        //"OR ( (STATUS    IN    ('E0005',    'E0006'))           " +
        //"AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )      " +

        //"+              " +

        //"(Select COUNT(*)     NEWRECEIVED            " +
        //"FROM    COMPLAINT               " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND           " +
        //"((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
        //"        ))          AND         " +
        //"(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND                " +
        //"    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      TOTALHTWIRE         " +

        //"FROM    DUAL)       TOTALHTWIRE,            " +

        //"(Select COUNT(*)     RESOLVEDHTWIRE         " +
        //"FROM    COMPLAINT       " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND   (STATUS IN  ('E0005',   'E0006'))   AND         " +
        //"((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
        //"        ))          AND             " +
        //"(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND                 " +
        //"    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      RESOLVEDHTWIRE,         " +

        //"(SELECT             " +

        //"(Select COUNT(*)     PreviousBalance        " +
        //"FROM    COMPLAINT               " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND           " +
        //"((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND       " +
        //"((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
        //"        ))              AND         " +
        //"( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          " +
        //"((STATUS    IN    ('E0005',    'E0006'))        AND             " +
        //"(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )              " +
        //"OR ( (STATUS    IN    ('E0005',    'E0006'))            " +
        //"AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )           " +

        //"+           " +

        //"(Select COUNT(*)     NEWRECEIVED        " +
        //"FROM    COMPLAINT           " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND           " +
        //"((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
        //"        ))          AND         " +
        //"(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
        //"    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          " +

        //"-           " +

        //"(Select COUNT(*)     RESOLVED               " +
        //"FROM    COMPLAINT               " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND   (STATUS IN  ('E0005',   'E0006'))   AND         " +
        //"((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'              " +
        //"        ))          AND             " +
        //"(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND                 " +
        //"    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      PENDINGHTWIRE               " +

        //"FROM    DUAL)       PENDINGHTWIRE,          " +



        //"(Select COUNT(*)     PreviousBalanceLTCableFault            " +
        //"FROM    COMPLAINT           " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND           " +
        //"((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND           " +
        //"((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'            " +
        //"        ))              AND         " +
        //"( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          " +
        //"((STATUS    IN    ('E0005',    'E0006'))        AND    " +
        //"(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )     " +
        //"OR ( (STATUS    IN    ('E0005',    'E0006'))       " +
        //"AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ))    PreviousBalanceLTCableFault,       " +

        //"(Select COUNT(*)     NEWRECEIVEDLTCableFault       " +
        //"FROM    COMPLAINT      " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND      " +
        //"((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'       " +
        //"        ))          AND        " +
        //"(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND           " +
        //"    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          NEWRECEIVEDLTCableFault,       " +

        //"(SELECT            " +

        //"(Select COUNT(*)     PreviousBalance           " +
        //"FROM    COMPLAINT          " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND      " +
        //"((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND      " +
        //"((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
        //"        ))              AND            " +
        //"( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR         " +
        //"((STATUS    IN    ('E0005',    'E0006'))        AND        " +
        //"(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )         " +
        //"OR ( (STATUS    IN    ('E0005',    'E0006'))       " +
        //"AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )      " +

        //"+          " +

        //"(Select COUNT(*)     NEWRECEIVED       " +
        //"FROM    COMPLAINT          " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND      " +
        //"((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'       " +
        //"        ))          AND            " +
        //"(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND           " +
        //"    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      TOTALLTCableFault          " +

        //"FROM    DUAL)       TOTALLTCableFault,         " +

        //"(Select COUNT(*)     RESOLVEDLTCableFault          " +
        //"FROM    COMPLAINT          " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND   (STATUS IN  ('E0005',   'E0006'))   AND        " +
        //"((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
        //"        ))          AND            " +
        //"(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
        //"    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      RESOLVEDLTCableFault,          " +

        //"(SELECT                " +

        //"(Select COUNT(*)     PreviousBalance           " +
        //"FROM    COMPLAINT              " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND          " +
        //"((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND      " +
        //"((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
        //"        ))              AND                " +
        //"( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR             " +
        //"((STATUS    IN    ('E0005',    'E0006'))        AND            " +
        //"(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )         " +
        //"OR ( (STATUS    IN    ('E0005',    'E0006'))           " +
        //"AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )      " +

        //"+          " +

        //"(Select COUNT(*)     NEWRECEIVED               " +
        //"FROM    COMPLAINT          " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND          " +
        //"((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
        //"        ))          AND                " +
        //"(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND               " +
        //"    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))             " +

        //"-              " +

        //"(Select COUNT(*)     RESOLVED              " +
        //"FROM    COMPLAINT              " +
        //"where (IBCCODE  =   '" + ibcCode + "')    AND   (STATUS IN  ('E0005',   'E0006'))   AND            " +
        //"((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
        //"        ))          AND        " +
        //"(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
        //"    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      PENDINGLTCableFault            " +

        //"FROM    DUAL)       PENDINGLTCableFault        " +




        //"FROM    DUAL";


        //                //Adding Parameters
        //                // _commnadData.AddParameter("@UserName", userID);

        //                //opening connection
        //                _commnadData.OpenWithOutTrans();

        //                //Executing Query
        //                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

        //                return _ds.Tables[0];
        //            }
        //            catch (Exception ex)
        //            {
        //                //Console.WriteLine("No record found");
        //                throw ex;
        //            }
        //            finally
        //            {
        //                //Console.WriteLine("No ");
        //                _commnadData.Close();

        //            }
        //        }



        //        public DataTable GetLTPendingTicketsDetailsForHourlySummaryReport(string _dateFrom, string _dateTo)
        //        {
        //            //Creating object of DAL class
        //            CommandData _commnadData = new CommandData();

        //            try
        //            {
        //                _commnadData._CommandType = CommandType.Text;

        //                //                _commnadData.CommandText = "SELECT '" + mncName + "'    MNCNAME,    CT.ComplaintTypeName FaultType, Count(C.ComplaintId) NOOFTICKET     " +
        //                //"FROM     FOURHOURCOMPLAINTSUMMARY C        " +
        //                //"inner join ComplaintType  CT  on C.ComplaintType= CT.ComplaintTypeCode         " +
        //                //"Where   (C.MNCName  =   '" + mncName + "')      AND       " +
        //                //"        ((C.ParentTicketNo is null OR C.ParentTicketNo='0'))        AND        " +
        //                //"        (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND        " +
        //                //"TO_DATE(C.ComplaintDateTime,'yyyyMMddHH24MIss') between TO_DATE('" + _dateFrom + "','yyyyMMddHH24MIss') AND TO_DATE('" + _dateTo + "','yyyyMMddHH24MIss')          " +
        //                //"   Group by MNCNAME ,CT.ComplaintTypeName ";



        //                _commnadData.CommandText = "SELECT  cn.MncName as \"MNC Name\", ComplaintTypeCode as \"Fault Type\",count(ComplaintTypeCode) AS \"No of Ticket\" FROM Complaint C inner join Centre cn On C.MncCode=cn.MNCCODE "+
        //" inner join Department D on CN.DEPTID=D.DEPTID  "+

        //" WHERE  ComplaintDateTime is not null AND trim(ComplaintDateTime) !='0' and  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') BETWEEN To_Date('" + _dateFrom + "','yyyyMMddHH24MIss') AND To_Date('" + _dateTo + "','yyyyMMddHH24MIss') " +
        //" group by  cn.MncName,ComplaintTypeCode "+
        //"order by  cn.MncName";


        //                //Adding Parameters
        //                // _commnadData.AddParameter("@UserName", userID);

        //                //opening connection
        //                _commnadData.OpenWithOutTrans();

        //                //Executing Query
        //                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

        //                return _ds.Tables[0];
        //            }
        //            catch (Exception ex)
        //            {
        //                //Console.WriteLine("No record found");
        //                throw ex;
        //            }
        //            finally
        //            {
        //                //Console.WriteLine("No ");
        //                _commnadData.Close();

        //            }
        //        }


        //        public DataTable GetOPNHoulySummaryReport(string dateFrom, string dateTo)
        //        {
        //            //Creating object of DAL class
        //            CommandData _commnadData = new CommandData();

        //            try
        //            {
        //                _commnadData._CommandType = CommandType.Text;
        //                _commnadData.CommandText = "select * from (select 	Distinct dts.FEEDERNAME, ord.OUTAGE   from OUTAGERECORD ord LEFT JOIN DTSDETAILS dts ON dts.FEEDERID = ord.FEEDERID   where   FEEDERNAME IS NOT NULL AND To_Date(To_Char(DateTime,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') between To_Date('" + dateFrom + "','yyyyMMddHH24MIss') AND  To_Date('" + dateTo + "','yyyyMMddHH24MIss')  AND Trim(Outage) in  ('FAULT','SHUTDOWN') Group BY ord.FEEDERID,ord.OUTAGE, dts.FEEDERNAME ) PIVOT ( COUNT(Outage)  FOR Outage IN ('FAULT' AS FAULT,'SHUTDOWN' AS SHUTDOWN ))";

        //                //Adding Parameters
        //                // _commnadData.AddParameter("@UserName", userID);

        //                //opening connection
        //                _commnadData.OpenWithOutTrans();

        //                //Executing Query
        //                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

        //                return _ds.Tables[0];
        //            }
        //            catch (Exception ex)
        //            {
        //                //Console.WriteLine("No record found");
        //                throw ex;
        //            }
        //            finally
        //            {
        //                //Console.WriteLine("No ");
        //                _commnadData.Close();

        //            }
        //        }
        //        #endregion

        #region HourlySummaryReport
        public DataTable GetCRMTicketsSummaryReport(string ibcCode, string _dateFrom, string _dateTo, string querystring)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (querystring != "")
                {
                    if (ibcCode != "" && ibcCode != "0")
                    {
                        _commnadData.CommandText = "SELECT  ( " +
                            " Select COUNT(*)     PreviousBalance        FROM    COMPLAINT    " +
                            " where " + querystring + " AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )   AND               " +
                            " ((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND    " +
                            " ( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          ((STATUS    IN    ('E0005',    'E0006'))        AND     " +
                            " (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND   " +
                            " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )          OR ( (STATUS    IN    ('E0005',    'E0006'))        AND  " +
                            " (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )   PB,  " +

                            " (Select COUNT(*)     NEWRECEIVED        FROM    COMPLAINT  " +
                            " where " + querystring + " AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND       " +
                            " (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND  " +
                            " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))  NR,         " +

                            " (Select COUNT(*)     RESOLVED               FROM    COMPLAINT    " +
                            " where " + querystring + " AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND   " +
                            " (STATUS IN  ('E0005',   'E0006'))   AND     " +
                            " (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND " +
                            " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))  RESOLVED,           " +

                            " (SELECT                 (Select COUNT(*)     PreviousBalance            FROM    COMPLAINT         " +
                            " where " + querystring + " AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND           " +
                            " ((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND" +
                            " ( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          ((STATUS    IN    ('E0005',    'E0006'))        AND  " +
                            " (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND  " +
                            " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )          OR ( (STATUS    IN    ('E0005',    'E0006'))            AND  " +
                            " (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) ) " +
                            "  +      " +
                            " (Select COUNT(*)     NEWRECEIVED            FROM    COMPLAINT  " +
                            " where " + querystring + " AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND               " +
                            " (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND  " +
                            "  TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      TOTAL           FROM    dual)   TOTAL,    " +

                            " (SELECT      COUNT(*) " +
                            " FROM        COMPLAINT " +
                            " WHERE  " + querystring + "  AND   (TRIM(MNCCODE) IN (" + ibcCode + ")  )           AND " +
                            "  (" +
                            "  (STATUS IN  ('E0005',   'E0006')    AND      " +
                            "  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) " +

                            "  OR" +

                            "  (STATUS NOT IN  ('E0005',   'E0006') " +
                            " )" +

                            "   )   AND" +
                            "   (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <   " +
                            " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))   PENDING,   " +

                            " (SELECT      COUNT(*)" +
                            " FROM        COMPLAINT" +
                            " WHERE      " + querystring + " AND  (TRIM(MNCCODE) IN (" + ibcCode + ")  )          AND" +
                            "  (" +
                            "    (STATUS IN  ('E0005',   'E0006')    AND " +
                            "   (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))" +

                            "   OR" +

                            "  (STATUS NOT IN  ('E0005',   'E0006') " +
                            "  )" +

                            "  )   AND" +
                            "  (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))    " +
                            "  AND" +
                            "  ((round(to_number(TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')  -   TO_DATE(ComplaintDateTime,  'yyyyMMddHH24MIss')) * 24)    >=  8)))      PENDING08HRS, " +


                            " (SELECT      Count(*)" +
                            " FROM        COMPLAINT " +
                            " WHERE   " + querystring + " AND    (TRIM(MNCCODE) IN (" + ibcCode + ")  )           AND " +
                            "    ( " +
                            "  (STATUS IN  ('E0005',   'E0006')    AND   " +
                            "  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) " +

                            " OR " +

                            " (STATUS NOT IN  ('E0005',   'E0006')  " +
                            "  ) " +

                            " )   AND " +
                            "  (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))    " +
                            " AND " +
                            "  (STATUSTO   =   'OPN'))       PENDINGHT, " +

                            " (SELECT      Count(*) " +
                            " FROM        COMPLAINT " +
                            " WHERE  " + querystring + "   AND  (TRIM(MNCCODE) IN (" + ibcCode + ")  )           AND " +
                            " ( " +
                            "   (STATUS IN  ('E0005',   'E0006')    AND  " +
                            "  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) " +

                            " OR " +

                            "  (STATUS NOT IN  ('E0005',   'E0006') " +
                            " ) " +

                            "  )   AND " +
                            " (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))    " +
                            " AND" +
                            " ((TRIM(STATUSTO)   !=   'OPN'    OR  TRIM(STATUSTO)    =   ''  OR  STATUSTO    IS  NULL)))   PENDINGLT               FROM    DUAL";
                    }
                    else
                    {
                        _commnadData.CommandText = "SELECT  ( " +
                            " Select COUNT(*)     PreviousBalance        FROM    COMPLAINT    " +
                            " where " + querystring + " AND               " +
                            " ((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND    " +
                            " ( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          ((STATUS    IN    ('E0005',    'E0006'))        AND     " +
                            " (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND   " +
                            " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )          OR ( (STATUS    IN    ('E0005',    'E0006'))        AND  " +
                            " (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )   PB,  " +

                            " (Select COUNT(*)     NEWRECEIVED        FROM    COMPLAINT  " +
                            " where " + querystring + " AND  " +
                            " (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND  " +
                            " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))  NR,         " +

                            " (Select COUNT(*)     RESOLVED               FROM    COMPLAINT    " +
                            " where " + querystring + " AND    " +
                            " (STATUS IN  ('E0005',   'E0006'))   AND     " +
                            " (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND " +
                            " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))  RESOLVED,           " +

                            " (SELECT                 (Select COUNT(*)     PreviousBalance            FROM    COMPLAINT         " +
                            " where " + querystring + " AND          " +
                            " ((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND" +
                            " ( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          ((STATUS    IN    ('E0005',    'E0006'))        AND  " +
                            " (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND  " +
                            " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )          OR ( (STATUS    IN    ('E0005',    'E0006'))            AND  " +
                            " (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) ) " +
                            "  +      " +
                            " (Select COUNT(*)     NEWRECEIVED            FROM    COMPLAINT  " +
                            " where " + querystring + " AND               " +
                            " (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND  " +
                            "  TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      TOTAL           FROM    dual)   TOTAL,    " +

                            " (SELECT      COUNT(*) " +
                            " FROM        COMPLAINT " +
                            " WHERE  " + querystring + "  AND  " +
                            "  (" +
                            "  (STATUS IN  ('E0005',   'E0006')    AND      " +
                            "  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) " +

                            "  OR" +

                            "  (STATUS NOT IN  ('E0005',   'E0006') " +
                            " )" +

                            "   )   AND" +
                            "   (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <   " +
                            " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))   PENDING,   " +

                            " (SELECT      COUNT(*)" +
                            " FROM        COMPLAINT" +
                            " WHERE      " + querystring + " AND  " +
                            "  (" +
                            "    (STATUS IN  ('E0005',   'E0006')    AND " +
                            "   (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))" +

                            "   OR" +

                            "  (STATUS NOT IN  ('E0005',   'E0006') " +
                            "  )" +

                            "  )   AND" +
                            "  (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))    " +
                            "  AND" +
                            "  ((round(to_number(TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')  -   TO_DATE(ComplaintDateTime,  'yyyyMMddHH24MIss')) * 24)    >=  8)))      PENDING08HRS, " +


                            " (SELECT      Count(*)" +
                            " FROM        COMPLAINT " +
                            " WHERE   " + querystring + " AND     " +
                            "    ( " +
                            "  (STATUS IN  ('E0005',   'E0006')    AND   " +
                            "  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) " +

                            " OR " +

                            " (STATUS NOT IN  ('E0005',   'E0006')  " +
                            "  ) " +

                            " )   AND " +
                            "  (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))    " +
                            " AND " +
                            "  (STATUSTO   =   'OPN'))       PENDINGHT, " +

                            " (SELECT      Count(*) " +
                            " FROM        COMPLAINT " +
                            " WHERE  " + querystring + "   AND   " +
                            " ( " +
                            "   (STATUS IN  ('E0005',   'E0006')    AND  " +
                            "  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) " +

                            " OR " +

                            "  (STATUS NOT IN  ('E0005',   'E0006') " +
                            " ) " +

                            "  )   AND " +
                            " (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))    " +
                            " AND" +
                            " ((TRIM(STATUSTO)   !=   'OPN'    OR  TRIM(STATUSTO)    =   ''  OR  STATUSTO    IS  NULL)))   PENDINGLT               FROM    DUAL";
                    }
                }
                else
                {
                    if (ibcCode != "" && ibcCode != "0")
                    {
                        _commnadData.CommandText = "SELECT  ( " +
     " Select COUNT(*)     PreviousBalance        FROM    COMPLAINT    " +
     " where (TRIM(MNCCODE) IN (" + ibcCode + ")  )   AND               " +
     " ((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND    " +
     " ( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          ((STATUS    IN    ('E0005',    'E0006'))        AND     " +
     " (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND   " +
     " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )          OR ( (STATUS    IN    ('E0005',    'E0006'))        AND  " +
     " (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )   PB,  " +

     " (Select COUNT(*)     NEWRECEIVED        FROM    COMPLAINT  " +
     " where (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND       " +
     " (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND  " +
     " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))  NR,         " +

     " (Select COUNT(*)     RESOLVED               FROM    COMPLAINT    " +
     " where (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND   " +
     " (STATUS IN  ('E0005',   'E0006'))   AND     " +
     " (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND " +
     " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))  RESOLVED,           " +

     " (SELECT                 (Select COUNT(*)     PreviousBalance            FROM    COMPLAINT         " +
     " where (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND           " +
     " ((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND" +
     " ( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          ((STATUS    IN    ('E0005',    'E0006'))        AND  " +
     " (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND  " +
     " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )          OR ( (STATUS    IN    ('E0005',    'E0006'))            AND  " +
     " (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) ) " +
     "  +      " +
      " (Select COUNT(*)     NEWRECEIVED            FROM    COMPLAINT  " +
      " where (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND               " +
      " (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND  " +
     "  TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      TOTAL           FROM    dual)   TOTAL,    " +

     " (SELECT      COUNT(*) " +
     " FROM        COMPLAINT " +
     " WHERE       (TRIM(MNCCODE) IN (" + ibcCode + ")  )         AND " +
                "  (" +
                    "  (STATUS IN  ('E0005',   'E0006')    AND      " +
                    "  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) " +

                    "  OR" +

                    "  (STATUS NOT IN  ('E0005',   'E0006') " +
                     " )" +

               "   )   AND" +
               "   (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <   " +
                 " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))   PENDING,   " +

     " (SELECT      COUNT(*)" +
     " FROM        COMPLAINT" +
     " WHERE       (TRIM(MNCCODE) IN (" + ibcCode + ")  )          AND" +
                "  (" +
                  "    (STATUS IN  ('E0005',   'E0006')    AND " +
                   "   (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))" +

                   "   OR" +

                    "  (STATUS NOT IN  ('E0005',   'E0006') " +
                    "  )" +

                "  )   AND" +
                "  (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))    " +
                "  AND" +
                "  ((round(to_number(TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')  -   TO_DATE(ComplaintDateTime,  'yyyyMMddHH24MIss')) * 24)    >=  8)))      PENDING08HRS, " +


     " (SELECT      Count(*)" +
     " FROM        COMPLAINT " +
     " WHERE       (TRIM(MNCCODE) IN (" + ibcCode + ")  )        AND " +
              "    ( " +
                    "  (STATUS IN  ('E0005',   'E0006')    AND   " +
                    "  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) " +

                     " OR " +

                     " (STATUS NOT IN  ('E0005',   'E0006')  " +
                    "  ) " +

                 " )   AND " +
                "  (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))    " +
                 " AND " +
                "  (STATUSTO   =   'OPN'))       PENDINGHT, " +

     " (SELECT      Count(*) " +
     " FROM        COMPLAINT " +
     " WHERE      (TRIM(MNCCODE) IN (" + ibcCode + ")  )          AND " +
                 " ( " +
                   "   (STATUS IN  ('E0005',   'E0006')    AND  " +
                    "  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) " +

                     " OR " +

                    "  (STATUS NOT IN  ('E0005',   'E0006') " +
                     " ) " +

                "  )   AND " +
                 " (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))    " +
                 " AND" +
                 " ((TRIM(STATUSTO)   !=   'OPN'    OR  TRIM(STATUSTO)    =   ''  OR  STATUSTO    IS  NULL)))   PENDINGLT               FROM    DUAL";
                    }
                    else
                    {
                        _commnadData.CommandText = "SELECT  ( " +
                             " Select COUNT(*)     PreviousBalance        FROM    COMPLAINT    " +
                             " where               " +
                             " ((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND    " +
                             " ( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          ((STATUS    IN    ('E0005',    'E0006'))        AND     " +
                             " (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND   " +
                             " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )          OR ( (STATUS    IN    ('E0005',    'E0006'))        AND  " +
                             " (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )   PB,  " +

                             " (Select COUNT(*)     NEWRECEIVED        FROM    COMPLAINT  " +
                             " where       " +
                             " (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND  " +
                             " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))  NR,         " +

                             " (Select COUNT(*)     RESOLVED               FROM    COMPLAINT    " +
                             " where   " +
                             " (STATUS IN  ('E0005',   'E0006'))   AND     " +
                             " (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND " +
                             " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))  RESOLVED,           " +

                             " (SELECT                 (Select COUNT(*)     PreviousBalance            FROM    COMPLAINT         " +
                             " where          " +
                             " ((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND" +
                             " ( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          ((STATUS    IN    ('E0005',    'E0006'))        AND  " +
                             " (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND  " +
                             " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )          OR ( (STATUS    IN    ('E0005',    'E0006'))            AND  " +
                             " (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) ) " +
                             "  +      " +
                              " (Select COUNT(*)     NEWRECEIVED            FROM    COMPLAINT  " +
                              " where               " +
                              " (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND  " +
                             "  TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      TOTAL           FROM    dual)   TOTAL,    " +

                             " (SELECT      COUNT(*) " +
                             " FROM        COMPLAINT " +
                             " WHERE        " +
                                        "  (" +
                                            "  (STATUS IN  ('E0005',   'E0006')    AND      " +
                                            "  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) " +

                                            "  OR" +

                                            "  (STATUS NOT IN  ('E0005',   'E0006') " +
                                             " )" +

                                       "   )   AND" +
                                       "   (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <   " +
                                         " TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))   PENDING,   " +

                             " (SELECT      COUNT(*)" +
                             " FROM        COMPLAINT" +
                             " WHERE              " +
                                        "  (" +
                                          "    (STATUS IN  ('E0005',   'E0006')    AND " +
                                           "   (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))" +

                                           "   OR" +

                                            "  (STATUS NOT IN  ('E0005',   'E0006') " +
                                            "  )" +

                                        "  )   AND" +
                                        "  (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))    " +
                                        "  AND" +
                                        "  ((round(to_number(TO_DATE('" + _dateTo + "',  'yyyyMMddHH24MIss')  -   TO_DATE(ComplaintDateTime,  'yyyyMMddHH24MIss')) * 24)    >=  8)))      PENDING08HRS, " +


                             " (SELECT      Count(*)" +
                             " FROM        COMPLAINT " +
                             " WHERE       " +
                                      "    ( " +
                                            "  (STATUS IN  ('E0005',   'E0006')    AND   " +
                                            "  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) " +

                                             " OR " +

                                             " (STATUS NOT IN  ('E0005',   'E0006')  " +
                                            "  ) " +

                                         " )   AND " +
                                        "  (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))    " +
                                         " AND " +
                                        "  (STATUSTO   =   'OPN'))       PENDINGHT, " +

                             " (SELECT      Count(*) " +
                             " FROM        COMPLAINT " +
                             " WHERE       " +
                                         " ( " +
                                           "   (STATUS IN  ('E0005',   'E0006')    AND  " +
                                            "  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) " +

                                             " OR " +

                                            "  (STATUS NOT IN  ('E0005',   'E0006') " +
                                             " ) " +

                                        "  )   AND " +
                                         " (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))    " +
                                         " AND" +
                                         " ((TRIM(STATUSTO)   !=   'OPN'    OR  TRIM(STATUSTO)    =   ''  OR  STATUSTO    IS  NULL)))   PENDINGLT               FROM    DUAL";
                    }


                }
                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetLTHTWireAndLTCableFaultReport(string ibcCode, string _dateFrom, string _dateTo, string querystring)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (querystring != "")
                {
                    if (ibcCode != "" && ibcCode != "0")
                    {
                        _commnadData.CommandText = "SELECT      " +

                                            "(Select COUNT(*)     PreviousBalanceLTWIRE          " +
                                            "FROM    COMPLAINT               " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + " AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND           " +
                                            "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND       " +
                                            "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR             " +
                                            "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')         " +
                                            "        ))              AND         " +
                                            "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          " +
                                            "((STATUS    IN    ('E0005',    'E0006'))        AND             " +
                                            "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )          " +
                                            "OR ( (STATUS    IN    ('E0005',    'E0006'))        " +
                                            "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ))    PreviousBalanceLTWIRE,      " +

                                            "(Select COUNT(*)     NEWRECEIVEDLTWIRE          " +
                                            "FROM    COMPLAINT           " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )   AND           " +
                                            "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR             " +
                                            "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')     " +
                                            "        ))          AND         " +
                                            "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                            "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          NEWRECEIVEDLTWIRE,      " +

                                            "(SELECT     " +

                                            "(Select COUNT(*)     PreviousBalance        " +
                                            "FROM    COMPLAINT       " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND       " +
                                            "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND       " +
                                            "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR         " +
                                            "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')         " +
                                            "        ))              AND     " +
                                            "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR      " +
                                            "((STATUS    IN    ('E0005',    'E0006'))        AND             " +
                                            "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )    " +
                                            "OR ( (STATUS    IN    ('E0005',    'E0006'))    " +
                                            "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )       " +

                                            "+           " +

                                            "(Select COUNT(*)     NEWRECEIVED        " +
                                            "FROM    COMPLAINT       " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND           " +
                                            "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR             " +
                                            "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')                                                                     " +
                                            "        ))          AND         " +
                                            "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                            "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      TOTALLTWIRE     " +

                                            "FROM    DUAL)       TOTALLTWIRE,        " +

                                            "(Select COUNT(*)     RESOLVEDLTWIRE         " +
                                            "FROM    COMPLAINT           " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND   (STATUS IN  ('E0005',   'E0006'))   AND                                                                     " +
                                            "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR " +
                                            "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')" +
                                            "        ))          AND         " +
                                            "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND         " +
                                            "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      RESOLVEDLTWIRE,         " +

                                            "(SELECT             " +

                                            "(Select COUNT(*)     PreviousBalance        " +
                                            "FROM    COMPLAINT           " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND           " +
                                            "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND           " +
                                            "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR                 " +
                                            "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')         " +
                                            "        ))              AND             " +
                                            "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          " +
                                            "((STATUS    IN    ('E0005',    'E0006'))        AND                 " +
                                            "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )          " +
                                            "OR ( (STATUS    IN    ('E0005',    'E0006'))            " +
                                            "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )           " +

                                            "+               " +

                                            "(Select COUNT(*)     NEWRECEIVED            " +
                                            "FROM    COMPLAINT                   " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND           " +
                                            "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR             " +
                                            "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')         " +
                                            "        ))          AND                 " +
                                            "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                            "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          " +

                                            "-      " +

                                            "(Select COUNT(*)     RESOLVED          " +
                                            "FROM    COMPLAINT              " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND   (STATUS IN  ('E0005',   'E0006'))   AND            " +
                                            "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR        " +

                                            "    (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')       " +
                                             "       ))          AND            " +
                                            "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                            "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      PENDINGLTWIRE          " +

                                            "FROM    DUAL)       PENDINGLTWIRE,         " +



                                            "(Select COUNT(*)     PreviousBalanceHTWIRE         " +
                                            "FROM    COMPLAINT              " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND          " +
                                            "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND          " +
                                            "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'         " +
                                            "        ))              AND            " +
                                            "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR         " +
                                            "((STATUS    IN    ('E0005',    'E0006'))        AND        " +
                                            "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )         " +
                                            "OR ( (STATUS    IN    ('E0005',    'E0006'))           " +
                                            "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ))    PreviousBalanceHTWIRE,     " +

                                            "(Select COUNT(*)     NEWRECEIVEDHTWIRE         " +
                                            "FROM    COMPLAINT          " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND          " +
                                            "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'         " +
                                            "        ))          AND            " +
                                            "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND               " +
                                            "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          NEWRECEIVEDHTWIRE,         " +

                                            "(SELECT            " +

                                            "(Select COUNT(*)     PreviousBalance               " +
                                            "FROM    COMPLAINT              " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )   AND      " +
                                            "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND      " +
                                            "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'         " +
                                            "        ))              AND                " +
                                            "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR             " +
                                            "((STATUS    IN    ('E0005',    'E0006'))        AND        " +
                                            "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )         " +
                                            "OR ( (STATUS    IN    ('E0005',    'E0006'))           " +
                                            "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )      " +

                                            "+              " +

                                            "(Select COUNT(*)     NEWRECEIVED            " +
                                            "FROM    COMPLAINT               " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND           " +
                                            "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
                                            "        ))          AND         " +
                                            "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND                " +
                                            "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      TOTALHTWIRE         " +

                                            "FROM    DUAL)       TOTALHTWIRE,            " +

                                            "(Select COUNT(*)     RESOLVEDHTWIRE         " +
                                            "FROM    COMPLAINT       " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND   (STATUS IN  ('E0005',   'E0006'))   AND         " +
                                            "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
                                            "        ))          AND             " +
                                            "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND                 " +
                                            "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      RESOLVEDHTWIRE,         " +

                                            "(SELECT             " +

                                            "(Select COUNT(*)     PreviousBalance        " +
                                            "FROM    COMPLAINT               " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND           " +
                                            "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND       " +
                                            "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
                                            "        ))              AND         " +
                                            "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          " +
                                            "((STATUS    IN    ('E0005',    'E0006'))        AND             " +
                                            "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )              " +
                                            "OR ( (STATUS    IN    ('E0005',    'E0006'))            " +
                                            "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )           " +

                                            "+           " +

                                            "(Select COUNT(*)     NEWRECEIVED        " +
                                            "FROM    COMPLAINT           " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND           " +
                                            "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
                                            "        ))          AND         " +
                                            "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                            "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          " +

                                            "-           " +

                                            "(Select COUNT(*)     RESOLVED               " +
                                            "FROM    COMPLAINT               " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND   (STATUS IN  ('E0005',   'E0006'))   AND         " +
                                            "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'              " +
                                            "        ))          AND             " +
                                            "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND                 " +
                                            "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      PENDINGHTWIRE               " +

                                            "FROM    DUAL)       PENDINGHTWIRE,          " +



                                            "(Select COUNT(*)     PreviousBalanceLTCableFault            " +
                                            "FROM    COMPLAINT           " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND           " +
                                            "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND           " +
                                            "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'            " +
                                            "        ))              AND         " +
                                            "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          " +
                                            "((STATUS    IN    ('E0005',    'E0006'))        AND    " +
                                            "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )     " +
                                            "OR ( (STATUS    IN    ('E0005',    'E0006'))       " +
                                            "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ))    PreviousBalanceLTCableFault,       " +

                                            "(Select COUNT(*)     NEWRECEIVEDLTCableFault       " +
                                            "FROM     COMPLAINT      " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND      " +
                                            "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'       " +
                                            "        ))          AND        " +
                                            "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND           " +
                                            "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          NEWRECEIVEDLTCableFault,       " +

                                            "(SELECT            " +

                                            "(Select COUNT(*)     PreviousBalance           " +
                                            "FROM    COMPLAINT          " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND      " +
                                            "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND      " +
                                            "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
                                            "        ))              AND            " +
                                            "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR         " +
                                            "((STATUS    IN    ('E0005',    'E0006'))        AND        " +
                                            "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )         " +
                                            "OR ( (STATUS    IN    ('E0005',    'E0006'))       " +
                                            "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )      " +

                                            "+          " +

                                            "(Select COUNT(*)     NEWRECEIVED       " +
                                            "FROM    COMPLAINT          " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND      " +
                                            "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'       " +
                                            "        ))          AND            " +
                                            "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND           " +
                                            "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      TOTALLTCableFault          " +

                                            "FROM    DUAL)       TOTALLTCableFault,         " +

                                            "(Select COUNT(*)     RESOLVEDLTCableFault          " +
                                            "FROM    COMPLAINT          " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND   (STATUS IN  ('E0005',   'E0006'))   AND        " +
                                            "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
                                            "        ))          AND            " +
                                            "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                            "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      RESOLVEDLTCableFault,          " +

                                            "(SELECT                " +

                                            "(Select COUNT(*)     PreviousBalance           " +
                                            "FROM    COMPLAINT              " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND          " +
                                            "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND      " +
                                            "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
                                            "        ))              AND                " +
                                            "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR             " +
                                            "((STATUS    IN    ('E0005',    'E0006'))        AND            " +
                                            "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )         " +
                                            "OR ( (STATUS    IN    ('E0005',    'E0006'))           " +
                                            "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )      " +

                                            "+          " +

                                            "(Select COUNT(*)     NEWRECEIVED               " +
                                            "FROM    COMPLAINT          " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND          " +
                                            "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
                                            "        ))          AND                " +
                                            "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND               " +
                                            "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))             " +

                                            "-              " +

                                            "(Select COUNT(*)     RESOLVED              " +
                                            "FROM    COMPLAINT              " +
                                            "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "  AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND   (STATUS IN  ('E0005',   'E0006'))   AND            " +
                                            "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
                                            "        ))          AND        " +
                                            "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                            "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      PENDINGLTCableFault            " +

                                            "FROM    DUAL)       PENDINGLTCableFault        " +




                                            "FROM    DUAL";
                    }
                    else
                    {

                        _commnadData.CommandText = "SELECT      " +

                                        "(Select COUNT(*)     PreviousBalanceLTWIRE          " +
                                        "FROM    COMPLAINT               " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "   AND           " +
                                        "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND       " +
                                        "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR             " +
                                        "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')         " +
                                        "        ))              AND         " +
                                        "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          " +
                                        "((STATUS    IN    ('E0005',    'E0006'))        AND             " +
                                        "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )          " +
                                        "OR ( (STATUS    IN    ('E0005',    'E0006'))        " +
                                        "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ))    PreviousBalanceLTWIRE,      " +

                                        "(Select COUNT(*)     NEWRECEIVEDLTWIRE          " +
                                        "FROM    COMPLAINT           " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "   AND           " +
                                        "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR             " +
                                        "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')     " +
                                        "        ))          AND         " +
                                        "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                        "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          NEWRECEIVEDLTWIRE,      " +

                                        "(SELECT     " +

                                        "(Select COUNT(*)     PreviousBalance        " +
                                        "FROM    COMPLAINT       " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "     AND       " +
                                        "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND       " +
                                        "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR         " +
                                        "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')         " +
                                        "        ))              AND     " +
                                        "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR      " +
                                        "((STATUS    IN    ('E0005',    'E0006'))        AND             " +
                                        "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )    " +
                                        "OR ( (STATUS    IN    ('E0005',    'E0006'))    " +
                                        "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )       " +

                                        "+           " +

                                        "(Select COUNT(*)     NEWRECEIVED        " +
                                        "FROM    COMPLAINT       " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "    AND           " +
                                        "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR             " +
                                        "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')                                                                     " +
                                        "        ))          AND         " +
                                        "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                        "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      TOTALLTWIRE     " +

                                        "FROM    DUAL)       TOTALLTWIRE,        " +

                                        "(Select COUNT(*)     RESOLVEDLTWIRE         " +
                                        "FROM    COMPLAINT           " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "     AND   (STATUS IN  ('E0005',   'E0006'))   AND                                                                     " +
                                        "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR " +
                                        "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')" +
                                        "        ))          AND         " +
                                        "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND         " +
                                        "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      RESOLVEDLTWIRE,         " +

                                        "(SELECT             " +

                                        "(Select COUNT(*)     PreviousBalance        " +
                                        "FROM    COMPLAINT           " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "     AND           " +
                                        "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND           " +
                                        "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR                 " +
                                        "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')         " +
                                        "        ))              AND             " +
                                        "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          " +
                                        "((STATUS    IN    ('E0005',    'E0006'))        AND                 " +
                                        "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )          " +
                                        "OR ( (STATUS    IN    ('E0005',    'E0006'))            " +
                                        "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )           " +

                                        "+               " +

                                        "(Select COUNT(*)     NEWRECEIVED            " +
                                        "FROM    COMPLAINT                   " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "      AND           " +
                                        "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR             " +
                                        "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')         " +
                                        "        ))          AND                 " +
                                        "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                        "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          " +

                                        "-      " +

                                        "(Select COUNT(*)     RESOLVED          " +
                                        "FROM    COMPLAINT              " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "      AND   (STATUS IN  ('E0005',   'E0006'))   AND            " +
                                        "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR        " +

                                        "    (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')       " +
                                         "       ))          AND            " +
                                        "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                        "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      PENDINGLTWIRE          " +

                                        "FROM    DUAL)       PENDINGLTWIRE,         " +



                                        "(Select COUNT(*)     PreviousBalanceHTWIRE         " +
                                        "FROM    COMPLAINT              " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "     AND          " +
                                        "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND          " +
                                        "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'         " +
                                        "        ))              AND            " +
                                        "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR         " +
                                        "((STATUS    IN    ('E0005',    'E0006'))        AND        " +
                                        "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )         " +
                                        "OR ( (STATUS    IN    ('E0005',    'E0006'))           " +
                                        "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ))    PreviousBalanceHTWIRE,     " +

                                        "(Select COUNT(*)     NEWRECEIVEDHTWIRE         " +
                                        "FROM    COMPLAINT          " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "     AND          " +
                                        "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'         " +
                                        "        ))          AND            " +
                                        "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND               " +
                                        "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          NEWRECEIVEDHTWIRE,         " +

                                        "(SELECT            " +

                                        "(Select COUNT(*)     PreviousBalance               " +
                                        "FROM    COMPLAINT              " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "     AND      " +
                                        "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND      " +
                                        "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'         " +
                                        "        ))              AND                " +
                                        "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR             " +
                                        "((STATUS    IN    ('E0005',    'E0006'))        AND        " +
                                        "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )         " +
                                        "OR ( (STATUS    IN    ('E0005',    'E0006'))           " +
                                        "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )      " +

                                        "+              " +

                                        "(Select COUNT(*)     NEWRECEIVED            " +
                                        "FROM    COMPLAINT               " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "     AND           " +
                                        "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
                                        "        ))          AND         " +
                                        "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND                " +
                                        "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      TOTALHTWIRE         " +

                                        "FROM    DUAL)       TOTALHTWIRE,            " +

                                        "(Select COUNT(*)     RESOLVEDHTWIRE         " +
                                        "FROM    COMPLAINT       " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "    AND   (STATUS IN  ('E0005',   'E0006'))   AND         " +
                                        "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
                                        "        ))          AND             " +
                                        "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND                 " +
                                        "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      RESOLVEDHTWIRE,         " +

                                        "(SELECT             " +

                                        "(Select COUNT(*)     PreviousBalance        " +
                                        "FROM    COMPLAINT               " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "     AND           " +
                                        "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND       " +
                                        "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
                                        "        ))              AND         " +
                                        "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          " +
                                        "((STATUS    IN    ('E0005',    'E0006'))        AND             " +
                                        "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )              " +
                                        "OR ( (STATUS    IN    ('E0005',    'E0006'))            " +
                                        "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )           " +

                                        "+           " +

                                        "(Select COUNT(*)     NEWRECEIVED        " +
                                        "FROM    COMPLAINT           " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "      AND           " +
                                        "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
                                        "        ))          AND         " +
                                        "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                        "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          " +

                                        "-           " +

                                        "(Select COUNT(*)     RESOLVED               " +
                                        "FROM    COMPLAINT               " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "     AND   (STATUS IN  ('E0005',   'E0006'))   AND         " +
                                        "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'              " +
                                        "        ))          AND             " +
                                        "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND                 " +
                                        "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      PENDINGHTWIRE               " +

                                        "FROM    DUAL)       PENDINGHTWIRE,          " +



                                        "(Select COUNT(*)     PreviousBalanceLTCableFault            " +
                                        "FROM    COMPLAINT           " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "     AND           " +
                                        "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND           " +
                                        "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'            " +
                                        "        ))              AND         " +
                                        "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          " +
                                        "((STATUS    IN    ('E0005',    'E0006'))        AND    " +
                                        "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )     " +
                                        "OR ( (STATUS    IN    ('E0005',    'E0006'))       " +
                                        "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ))    PreviousBalanceLTCableFault,       " +

                                        "(Select COUNT(*)     NEWRECEIVEDLTCableFault       " +
                                        "FROM     COMPLAINT      " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "     AND      " +
                                        "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'       " +
                                        "        ))          AND        " +
                                        "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND           " +
                                        "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          NEWRECEIVEDLTCableFault,       " +

                                        "(SELECT            " +

                                        "(Select COUNT(*)     PreviousBalance           " +
                                        "FROM    COMPLAINT          " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "   AND      " +
                                        "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND      " +
                                        "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
                                        "        ))              AND            " +
                                        "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR         " +
                                        "((STATUS    IN    ('E0005',    'E0006'))        AND        " +
                                        "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )         " +
                                        "OR ( (STATUS    IN    ('E0005',    'E0006'))       " +
                                        "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )      " +

                                        "+          " +

                                        "(Select COUNT(*)     NEWRECEIVED       " +
                                        "FROM    COMPLAINT          " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "    AND      " +
                                        "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'       " +
                                        "        ))          AND            " +
                                        "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND           " +
                                        "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      TOTALLTCableFault          " +

                                        "FROM    DUAL)       TOTALLTCableFault,         " +

                                        "(Select COUNT(*)     RESOLVEDLTCableFault          " +
                                        "FROM    COMPLAINT          " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "     AND   (STATUS IN  ('E0005',   'E0006'))   AND        " +
                                        "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
                                        "        ))          AND            " +
                                        "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                        "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      RESOLVEDLTCableFault,          " +

                                        "(SELECT                " +

                                        "(Select COUNT(*)     PreviousBalance           " +
                                        "FROM    COMPLAINT              " +
                                        "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  " + querystring + "     AND          " +
                                        "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND      " +
                                        "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
                                        "        ))              AND                " +
                                        "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR             " +
                                        "((STATUS    IN    ('E0005',    'E0006'))        AND            " +
                                        "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )         " +
                                        "OR ( (STATUS    IN    ('E0005',    'E0006'))           " +
                                        "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )      " +

                                        "+          " +

                                        "(Select COUNT(*)     NEWRECEIVED               " +
                                        "FROM    COMPLAINT          " +
                                        "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "     AND          " +
                                        "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
                                        "        ))          AND                " +
                                        "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND               " +
                                        "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))             " +

                                        "-              " +

                                        "(Select COUNT(*)     RESOLVED              " +
                                        "FROM    COMPLAINT              " +
                                        "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " + querystring + "    AND   (STATUS IN  ('E0005',   'E0006'))   AND            " +
                                        "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
                                        "        ))          AND        " +
                                        "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                        "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      PENDINGLTCableFault            " +

                                        "FROM    DUAL)       PENDINGLTCableFault        " +




                                        "FROM    DUAL";
                    }
                }
                else
                {
                    if (ibcCode != "" && ibcCode != "0")
                    {
                        _commnadData.CommandText = "SELECT      " +

                                           "(Select COUNT(*)     PreviousBalanceLTWIRE          " +
                                           "FROM    COMPLAINT               " +
                                           "where (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND   (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND           " +
                                           "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND       " +
                                           "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR             " +
                                           "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')         " +
                                           "        ))              AND         " +
                                           "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          " +
                                           "((STATUS    IN    ('E0005',    'E0006'))        AND             " +
                                           "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )          " +
                                           "OR ( (STATUS    IN    ('E0005',    'E0006'))        " +
                                           "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ))    PreviousBalanceLTWIRE,      " +

                                           "(Select COUNT(*)     NEWRECEIVEDLTWIRE          " +
                                           "FROM    COMPLAINT           " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  (TRIM(MNCCODE) IN (" + ibcCode + ")  )   AND           " +
                                           "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR             " +
                                           "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')     " +
                                           "        ))          AND         " +
                                           "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                           "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          NEWRECEIVEDLTWIRE,      " +

                                           "(SELECT     " +

                                           "(Select COUNT(*)     PreviousBalance        " +
                                           "FROM    COMPLAINT       " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND       " +
                                           "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND       " +
                                           "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR         " +
                                           "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')         " +
                                           "        ))              AND     " +
                                           "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR      " +
                                           "((STATUS    IN    ('E0005',    'E0006'))        AND             " +
                                           "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )    " +
                                           "OR ( (STATUS    IN    ('E0005',    'E0006'))    " +
                                           "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )       " +

                                           "+           " +

                                           "(Select COUNT(*)     NEWRECEIVED        " +
                                           "FROM    COMPLAINT       " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND           " +
                                           "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR             " +
                                           "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')                                                                     " +
                                           "        ))          AND         " +
                                           "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                           "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      TOTALLTWIRE     " +

                                           "FROM    DUAL)       TOTALLTWIRE,        " +

                                           "(Select COUNT(*)     RESOLVEDLTWIRE         " +
                                           "FROM    COMPLAINT           " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND   (STATUS IN  ('E0005',   'E0006'))   AND                                                                     " +
                                           "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR " +
                                           "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')" +
                                           "        ))          AND         " +
                                           "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND         " +
                                           "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      RESOLVEDLTWIRE,         " +

                                           "(SELECT             " +

                                           "(Select COUNT(*)     PreviousBalance        " +
                                           "FROM    COMPLAINT           " +
                                           "where   (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND           " +
                                           "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND           " +
                                           "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR                 " +
                                           "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')         " +
                                           "        ))              AND             " +
                                           "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          " +
                                           "((STATUS    IN    ('E0005',    'E0006'))        AND                 " +
                                           "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )          " +
                                           "OR ( (STATUS    IN    ('E0005',    'E0006'))            " +
                                           "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )           " +

                                           "+               " +

                                           "(Select COUNT(*)     NEWRECEIVED            " +
                                           "FROM    COMPLAINT                   " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND           " +
                                           "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR             " +
                                           "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')         " +
                                           "        ))          AND                 " +
                                           "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                           "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          " +

                                           "-      " +

                                           "(Select COUNT(*)     RESOLVED          " +
                                           "FROM    COMPLAINT              " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND   (STATUS IN  ('E0005',   'E0006'))   AND            " +
                                           "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR        " +

                                           "    (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')       " +
                                            "       ))          AND            " +
                                           "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                           "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      PENDINGLTWIRE          " +

                                           "FROM    DUAL)       PENDINGLTWIRE,         " +



                                           "(Select COUNT(*)     PreviousBalanceHTWIRE         " +
                                           "FROM    COMPLAINT              " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND          " +
                                           "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND          " +
                                           "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'         " +
                                           "        ))              AND            " +
                                           "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR         " +
                                           "((STATUS    IN    ('E0005',    'E0006'))        AND        " +
                                           "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )         " +
                                           "OR ( (STATUS    IN    ('E0005',    'E0006'))           " +
                                           "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ))    PreviousBalanceHTWIRE,     " +

                                           "(Select COUNT(*)     NEWRECEIVEDHTWIRE         " +
                                           "FROM    COMPLAINT          " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND   (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND          " +
                                           "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'         " +
                                           "        ))          AND            " +
                                           "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND               " +
                                           "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          NEWRECEIVEDHTWIRE,         " +

                                           "(SELECT            " +

                                           "(Select COUNT(*)     PreviousBalance               " +
                                           "FROM    COMPLAINT              " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND      " +
                                           "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND      " +
                                           "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'         " +
                                           "        ))              AND                " +
                                           "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR             " +
                                           "((STATUS    IN    ('E0005',    'E0006'))        AND        " +
                                           "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )         " +
                                           "OR ( (STATUS    IN    ('E0005',    'E0006'))           " +
                                           "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )      " +

                                           "+              " +

                                           "(Select COUNT(*)     NEWRECEIVED            " +
                                           "FROM    COMPLAINT               " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )   AND           " +
                                           "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
                                           "        ))          AND         " +
                                           "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND                " +
                                           "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      TOTALHTWIRE         " +

                                           "FROM    DUAL)       TOTALHTWIRE,            " +

                                           "(Select COUNT(*)     RESOLVEDHTWIRE         " +
                                           "FROM    COMPLAINT       " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  (TRIM(MNCCODE) IN (" + ibcCode + ")  )   AND   (STATUS IN  ('E0005',   'E0006'))   AND         " +
                                           "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
                                           "        ))          AND             " +
                                           "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND                 " +
                                           "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      RESOLVEDHTWIRE,         " +

                                           "(SELECT             " +

                                           "(Select COUNT(*)     PreviousBalance        " +
                                           "FROM    COMPLAINT               " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND           " +
                                           "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND       " +
                                           "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
                                           "        ))              AND         " +
                                           "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          " +
                                           "((STATUS    IN    ('E0005',    'E0006'))        AND             " +
                                           "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )              " +
                                           "OR ( (STATUS    IN    ('E0005',    'E0006'))            " +
                                           "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )           " +

                                           "+           " +

                                           "(Select COUNT(*)     NEWRECEIVED        " +
                                           "FROM    COMPLAINT           " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND           " +
                                           "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
                                           "        ))          AND         " +
                                           "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                           "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          " +

                                           "-           " +

                                           "(Select COUNT(*)     RESOLVED               " +
                                           "FROM    COMPLAINT               " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  (TRIM(MNCCODE) IN (" + ibcCode + ")  )   AND   (STATUS IN  ('E0005',   'E0006'))   AND         " +
                                           "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'              " +
                                           "        ))          AND             " +
                                           "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND                 " +
                                           "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      PENDINGHTWIRE               " +

                                           "FROM    DUAL)       PENDINGHTWIRE,          " +



                                           "(Select COUNT(*)     PreviousBalanceLTCableFault            " +
                                           "FROM    COMPLAINT           " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND           " +
                                           "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND           " +
                                           "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'            " +
                                           "        ))              AND         " +
                                           "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          " +
                                           "((STATUS    IN    ('E0005',    'E0006'))        AND    " +
                                           "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )     " +
                                           "OR ( (STATUS    IN    ('E0005',    'E0006'))       " +
                                           "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ))    PreviousBalanceLTCableFault,       " +

                                           "(Select COUNT(*)     NEWRECEIVEDLTCableFault       " +
                                           "FROM     COMPLAINT      " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND      " +
                                           "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'       " +
                                           "        ))          AND        " +
                                           "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND           " +
                                           "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          NEWRECEIVEDLTCableFault,       " +

                                           "(SELECT            " +

                                           "(Select COUNT(*)     PreviousBalance           " +
                                           "FROM    COMPLAINT          " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND      " +
                                           "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND      " +
                                           "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
                                           "        ))              AND            " +
                                           "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR         " +
                                           "((STATUS    IN    ('E0005',    'E0006'))        AND        " +
                                           "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )         " +
                                           "OR ( (STATUS    IN    ('E0005',    'E0006'))       " +
                                           "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )      " +

                                           "+          " +

                                           "(Select COUNT(*)     NEWRECEIVED       " +
                                           "FROM    COMPLAINT          " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  (TRIM(MNCCODE) IN (" + ibcCode + ")  )   AND      " +
                                           "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'       " +
                                           "        ))          AND            " +
                                           "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND           " +
                                           "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      TOTALLTCableFault          " +

                                           "FROM    DUAL)       TOTALLTCableFault,         " +

                                           "(Select COUNT(*)     RESOLVEDLTCableFault          " +
                                           "FROM    COMPLAINT          " +
                                           "where   (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND   (STATUS IN  ('E0005',   'E0006'))   AND        " +
                                           "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
                                           "        ))          AND            " +
                                           "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                           "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      RESOLVEDLTCableFault,          " +

                                           "(SELECT                " +

                                           "(Select COUNT(*)     PreviousBalance           " +
                                           "FROM    COMPLAINT              " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  (TRIM(MNCCODE) IN (" + ibcCode + ")  )   AND          " +
                                           "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND      " +
                                           "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
                                           "        ))              AND                " +
                                           "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR             " +
                                           "((STATUS    IN    ('E0005',    'E0006'))        AND            " +
                                           "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )         " +
                                           "OR ( (STATUS    IN    ('E0005',    'E0006'))           " +
                                           "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )      " +

                                           "+          " +

                                           "(Select COUNT(*)     NEWRECEIVED               " +
                                           "FROM    COMPLAINT          " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND          " +
                                           "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
                                           "        ))          AND                " +
                                           "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND               " +
                                           "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))             " +

                                           "-              " +

                                           "(Select COUNT(*)     RESOLVED              " +
                                           "FROM    COMPLAINT              " +
                                           "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  (TRIM(MNCCODE) IN (" + ibcCode + ")  )    AND   (STATUS IN  ('E0005',   'E0006'))   AND            " +
                                           "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
                                           "        ))          AND        " +
                                           "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                           "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      PENDINGLTCableFault            " +

                                           "FROM    DUAL)       PENDINGLTCableFault        " +




                                           "FROM    DUAL";
                    }
                    else
                    {
                        _commnadData.CommandText = "SELECT      " +

                                         "(Select COUNT(*)     PreviousBalanceLTWIRE          " +
                                         "FROM    COMPLAINT               " +
                                         "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND           " +
                                         "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND       " +
                                         "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR             " +
                                         "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')         " +
                                         "        ))              AND         " +
                                         "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          " +
                                         "((STATUS    IN    ('E0005',    'E0006'))        AND             " +
                                         "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )          " +
                                         "OR ( (STATUS    IN    ('E0005',    'E0006'))        " +
                                         "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ))    PreviousBalanceLTWIRE,      " +

                                         "(Select COUNT(*)     NEWRECEIVEDLTWIRE          " +
                                         "FROM    COMPLAINT           " +
                                         "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND          " +
                                         "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR             " +
                                         "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')     " +
                                         "        ))          AND         " +
                                         "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                         "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          NEWRECEIVEDLTWIRE,      " +

                                         "(SELECT     " +

                                         "(Select COUNT(*)     PreviousBalance        " +
                                         "FROM    COMPLAINT       " +
                                         "where    (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND     " +
                                         "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND       " +
                                         "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR         " +
                                         "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')         " +
                                         "        ))              AND     " +
                                         "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR      " +
                                         "((STATUS    IN    ('E0005',    'E0006'))        AND             " +
                                         "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )    " +
                                         "OR ( (STATUS    IN    ('E0005',    'E0006'))    " +
                                         "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )       " +

                                         "+           " +

                                         "(Select COUNT(*)     NEWRECEIVED        " +
                                         "FROM    COMPLAINT       " +
                                         "where   (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND     " +
                                         "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR             " +
                                         "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')                                                                     " +
                                         "        ))          AND         " +
                                         "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                         "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      TOTALLTWIRE     " +

                                         "FROM    DUAL)       TOTALLTWIRE,        " +

                                         "(Select COUNT(*)     RESOLVEDLTWIRE         " +
                                         "FROM    COMPLAINT           " +
                                         "where   (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND   (STATUS IN  ('E0005',   'E0006'))   AND                                                                     " +
                                         "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR " +
                                         "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')" +
                                         "        ))          AND         " +
                                         "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND         " +
                                         "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      RESOLVEDLTWIRE,         " +

                                         "(SELECT             " +

                                         "(Select COUNT(*)     PreviousBalance        " +
                                         "FROM    COMPLAINT           " +
                                         "where      (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND      " +
                                         "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND           " +
                                         "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR                 " +
                                         "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')         " +
                                         "        ))              AND             " +
                                         "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          " +
                                         "((STATUS    IN    ('E0005',    'E0006'))        AND                 " +
                                         "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )          " +
                                         "OR ( (STATUS    IN    ('E0005',    'E0006'))            " +
                                         "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )           " +

                                         "+               " +

                                         "(Select COUNT(*)     NEWRECEIVED            " +
                                         "FROM    COMPLAINT                   " +
                                         "where      (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND   " +
                                         "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR             " +
                                         "           (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')         " +
                                         "        ))          AND                 " +
                                         "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                         "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          " +

                                         "-      " +

                                         "(Select COUNT(*)     RESOLVED          " +
                                         "FROM    COMPLAINT              " +
                                         "where   (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND (STATUS IN  ('E0005',   'E0006'))   AND            " +
                                         "((UPPER(TRIM(StatusReason)) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR        " +

                                         "    (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')       " +
                                          "       ))          AND            " +
                                         "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                         "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      PENDINGLTWIRE          " +

                                         "FROM    DUAL)       PENDINGLTWIRE,         " +



                                         "(Select COUNT(*)     PreviousBalanceHTWIRE         " +
                                         "FROM    COMPLAINT              " +
                                         "where    (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND        " +
                                         "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND          " +
                                         "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'         " +
                                         "        ))              AND            " +
                                         "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR         " +
                                         "((STATUS    IN    ('E0005',    'E0006'))        AND        " +
                                         "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )         " +
                                         "OR ( (STATUS    IN    ('E0005',    'E0006'))           " +
                                         "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ))    PreviousBalanceHTWIRE,     " +

                                         "(Select COUNT(*)     NEWRECEIVEDHTWIRE         " +
                                         "FROM    COMPLAINT          " +
                                         "where    (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND     " +
                                         "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'         " +
                                         "        ))          AND            " +
                                         "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND               " +
                                         "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          NEWRECEIVEDHTWIRE,         " +

                                         "(SELECT            " +

                                         "(Select COUNT(*)     PreviousBalance               " +
                                         "FROM    COMPLAINT              " +
                                         "where     (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND   " +
                                         "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND      " +
                                         "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'         " +
                                         "        ))              AND                " +
                                         "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR             " +
                                         "((STATUS    IN    ('E0005',    'E0006'))        AND        " +
                                         "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )         " +
                                         "OR ( (STATUS    IN    ('E0005',    'E0006'))           " +
                                         "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )      " +

                                         "+              " +

                                         "(Select COUNT(*)     NEWRECEIVED            " +
                                         "FROM    COMPLAINT               " +
                                         "where     (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND   " +
                                         "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
                                         "        ))          AND         " +
                                         "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND                " +
                                         "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      TOTALHTWIRE         " +

                                         "FROM    DUAL)       TOTALHTWIRE,            " +

                                         "(Select COUNT(*)     RESOLVEDHTWIRE         " +
                                         "FROM    COMPLAINT       " +
                                         "where   (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND   (STATUS IN  ('E0005',   'E0006'))   AND         " +
                                         "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
                                         "        ))          AND             " +
                                         "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND                 " +
                                         "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      RESOLVEDHTWIRE,         " +

                                         "(SELECT             " +

                                         "(Select COUNT(*)     PreviousBalance        " +
                                         "FROM    COMPLAINT               " +
                                         "where    (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND        " +
                                         "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND       " +
                                         "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
                                         "        ))              AND         " +
                                         "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          " +
                                         "((STATUS    IN    ('E0005',    'E0006'))        AND             " +
                                         "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )              " +
                                         "OR ( (STATUS    IN    ('E0005',    'E0006'))            " +
                                         "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )           " +

                                         "+           " +

                                         "(Select COUNT(*)     NEWRECEIVED        " +
                                         "FROM    COMPLAINT           " +
                                         "where     (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND        " +
                                         "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'          " +
                                         "        ))          AND         " +
                                         "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                         "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          " +

                                         "-           " +

                                         "(Select COUNT(*)     RESOLVED               " +
                                         "FROM    COMPLAINT               " +
                                         "where  (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND   (STATUS IN  ('E0005',   'E0006'))   AND         " +
                                         "((UPPER(TRIM(StatusReason)) =    'HT WIRE BROKEN/REPAIRED'              " +
                                         "        ))          AND             " +
                                         "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND                 " +
                                         "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      PENDINGHTWIRE               " +

                                         "FROM    DUAL)       PENDINGHTWIRE,          " +



                                         "(Select COUNT(*)     PreviousBalanceLTCableFault            " +
                                         "FROM    COMPLAINT           " +
                                         "where    (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND       " +
                                         "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND           " +
                                         "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'            " +
                                         "        ))              AND         " +
                                         "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR          " +
                                         "((STATUS    IN    ('E0005',    'E0006'))        AND    " +
                                         "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )     " +
                                         "OR ( (STATUS    IN    ('E0005',    'E0006'))       " +
                                         "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ))    PreviousBalanceLTCableFault,       " +

                                         "(Select COUNT(*)     NEWRECEIVEDLTCableFault       " +
                                         "FROM     COMPLAINT      " +
                                         "where    (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND   " +
                                         "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'       " +
                                         "        ))          AND        " +
                                         "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND           " +
                                         "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))          NEWRECEIVEDLTCableFault,       " +

                                         "(SELECT            " +

                                         "(Select COUNT(*)     PreviousBalance           " +
                                         "FROM    COMPLAINT          " +
                                         "where     (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND " +
                                         "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND      " +
                                         "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
                                         "        ))              AND            " +
                                         "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR         " +
                                         "((STATUS    IN    ('E0005',    'E0006'))        AND        " +
                                         "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )         " +
                                         "OR ( (STATUS    IN    ('E0005',    'E0006'))       " +
                                         "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )      " +

                                         "+          " +

                                         "(Select COUNT(*)     NEWRECEIVED       " +
                                         "FROM    COMPLAINT          " +
                                         "where    (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND     " +
                                         "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'       " +
                                         "        ))          AND            " +
                                         "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND           " +
                                         "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      TOTALLTCableFault          " +

                                         "FROM    DUAL)       TOTALLTCableFault,         " +

                                         "(Select COUNT(*)     RESOLVEDLTCableFault          " +
                                         "FROM    COMPLAINT          " +
                                         "where    (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  (STATUS IN  ('E0005',   'E0006'))   AND        " +
                                         "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
                                         "        ))          AND            " +
                                         "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                         "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      RESOLVEDLTCableFault,          " +

                                         "(SELECT                " +

                                         "(Select COUNT(*)     PreviousBalance           " +
                                         "FROM    COMPLAINT              " +
                                         "where   (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND         " +
                                         "((TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')))        AND      " +
                                         "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
                                         "        ))              AND                " +
                                         "( (STATUS    NOT    IN    ('E0005',    'E0006'))        OR             " +
                                         "((STATUS    IN    ('E0005',    'E0006'))        AND            " +
                                         "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN        TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')) )         " +
                                         "OR ( (STATUS    IN    ('E0005',    'E0006'))           " +
                                         "AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss'))) ) )      " +

                                         "+          " +

                                         "(Select COUNT(*)     NEWRECEIVED               " +
                                         "FROM    COMPLAINT          " +
                                         "where   (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND         " +
                                         "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
                                         "        ))          AND                " +
                                         "(TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND               " +
                                         "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))             " +

                                         "-              " +

                                         "(Select COUNT(*)     RESOLVED              " +
                                         "FROM    COMPLAINT              " +
                                         "where   (Trim(ParentTicketno)='0' OR ParentTicketNo is null) AND  (STATUS IN  ('E0005',   'E0006'))   AND            " +
                                         "((UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED'           " +
                                         "        ))          AND        " +
                                         "(TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    BETWEEN    TO_DATE('" + _dateFrom + "',    'yyyyMMddHH24MIss')    AND            " +
                                         "    TO_DATE('" + _dateTo + "',    'yyyyMMddHH24MIss')))      PENDINGLTCableFault            " +

                                         "FROM    DUAL)       PENDINGLTCableFault        " +




                                         "FROM    DUAL";
                    }




                }

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetLTPendingTicketsDetailsForHourlySummaryReport(string ibcCode,string _dateFrom, string _dateTo)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;

                //                _commnadData.CommandText = "SELECT '" + mncName + "'    MNCNAME,    CT.ComplaintTypeName FaultType, Count(C.ComplaintId) NOOFTICKET     " +
                //"FROM     FOURHOURCOMPLAINTSUMMARY C        " +
                //"inner join ComplaintType  CT  on C.ComplaintType= CT.ComplaintTypeCode         " +
                //"Where   (C.MNCName  =   '" + mncName + "')      AND       " +
                //"        ((C.ParentTicketNo is null OR C.ParentTicketNo='0'))        AND        " +
                //"        (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND        " +
                //"TO_DATE(C.ComplaintDateTime,'yyyyMMddHH24MIss') between TO_DATE('" + _dateFrom + "','yyyyMMddHH24MIss') AND TO_DATE('" + _dateTo + "','yyyyMMddHH24MIss')          " +
                //"   Group by MNCNAME ,CT.ComplaintTypeName ";

                if(ibcCode!="" && ibcCode !=null)
                {
                    _commnadData.CommandText = "SELECT  cn.MncName as \"MNC Name\", ComplaintTypeCode as \"Fault Type\",count(ComplaintTypeCode) AS \"No of Ticket\" FROM Complaint C inner join Centre cn On C.MncCode=cn.MNCCODE AND trim(c.MNCCODE) in ("+ibcCode+") " +
" inner join Department D on CN.DEPTID=D.DEPTID  " +

" WHERE  ComplaintDateTime is not null AND trim(ComplaintDateTime) !='0' and  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') BETWEEN To_Date('" + _dateFrom + "','yyyyMMddHH24MIss') AND To_Date('" + _dateTo + "','yyyyMMddHH24MIss') " +
" group by  cn.MncName,ComplaintTypeCode " +
"order by  cn.MncName";
                }
                else
                {
                    _commnadData.CommandText = "SELECT  cn.MncName as \"MNC Name\", ComplaintTypeCode as \"Fault Type\",count(ComplaintTypeCode) AS \"No of Ticket\" FROM Complaint C inner join Centre cn On C.MncCode=cn.MNCCODE " +
                  " inner join Department D on CN.DEPTID=D.DEPTID  " +

                  " WHERE  ComplaintDateTime is not null AND trim(ComplaintDateTime) !='0' and  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') BETWEEN To_Date('" + _dateFrom + "','yyyyMMddHH24MIss') AND To_Date('" + _dateTo + "','yyyyMMddHH24MIss') " +
                  " group by  cn.MncName,ComplaintTypeCode " +
                  "order by  cn.MncName";
                }

              


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        public DataTable GetOPNHoulySummaryReport(string ibcCode, string _dateFrom, string _dateTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;

                //                _commnadData.CommandText = "SELECT '" + mncName + "'    MNCNAME,    CT.ComplaintTypeName FaultType, Count(C.ComplaintId) NOOFTICKET     " +
                //"FROM     FOURHOURCOMPLAINTSUMMARY C        " +
                //"inner join ComplaintType  CT  on C.ComplaintType= CT.ComplaintTypeCode         " +
                //"Where   (C.MNCName  =   '" + mncName + "')      AND       " +
                //"        ((C.ParentTicketNo is null OR C.ParentTicketNo='0'))        AND        " +
                //"        (SummaryAt  =   To_Date('" + summaryAt + "',   'MM/DD/yyyy HH12:MI:ss AM'))      AND        " +
                //"TO_DATE(C.ComplaintDateTime,'yyyyMMddHH24MIss') between TO_DATE('" + _dateFrom + "','yyyyMMddHH24MIss') AND TO_DATE('" + _dateTo + "','yyyyMMddHH24MIss')          " +
                //"   Group by MNCNAME ,CT.ComplaintTypeName ";
                if (QueryString != "")
                {
                  
                        if (ibcCode != "" && ibcCode != null)
                        {
                            _commnadData.CommandText = "SELECT  cn.MncName as \"MNC Name\", count(StatusTo) AS \"OPN Ticket\" FROM Complaint C inner join Centre cn On C.MncCode=cn.MNCCODE AND trim(c.MNCCODE) in (" + ibcCode + ") " +
        " inner join Department D on CN.DEPTID=D.DEPTID  " +

        " WHERE  ComplaintDateTime is not null AND trim(ComplaintDateTime) !='0'  and  trim(StatusTo)='OPN' and   To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') BETWEEN To_Date('" + _dateFrom + "','yyyyMMddHH24MIss') AND To_Date('" + _dateTo + "','yyyyMMddHH24MIss') AND " + QueryString+
        " group by  cn.MncName " +
        "order by  cn.MncName";
                        }
                        else
                        {
                            _commnadData.CommandText = "SELECT  cn.MncName as \"MNC Name\", count(StatusTo) AS \"OPN Ticket\" FROM Complaint C inner join Centre cn On C.MncCode=cn.MNCCODE " +
                          " inner join Department D on CN.DEPTID=D.DEPTID  " +

                          " WHERE  ComplaintDateTime is not null AND trim(ComplaintDateTime) !='0' and  trim(StatusTo)='OPN' and  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') BETWEEN To_Date('" + _dateFrom + "','yyyyMMddHH24MIss') AND To_Date('" + _dateTo + "','yyyyMMddHH24MIss') AND " +QueryString+
                          " group by  cn.MncName " +
                          "order by  cn.MncName";
                        }
                    
                }
                else
                {
                   
                        if (ibcCode != "" && ibcCode != null)
                        {
                            _commnadData.CommandText = "SELECT  cn.MncName as \"MNC Name\", count(StatusTo) AS \"OPN Ticket\" FROM Complaint C inner join Centre cn On C.MncCode=cn.MNCCODE AND trim(c.MNCCODE) in (" + ibcCode + ") " +
        " inner join Department D on CN.DEPTID=D.DEPTID  " +

        " WHERE  ComplaintDateTime is not null AND trim(ComplaintDateTime) !='0'  and  trim(StatusTo)='OPN' and   To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') BETWEEN To_Date('" + _dateFrom + "','yyyyMMddHH24MIss') AND To_Date('" + _dateTo + "','yyyyMMddHH24MIss')  " + 
        " group by  cn.MncName " +
        "order by  cn.MncName";
                        }
                        else
                        {
                            _commnadData.CommandText = "SELECT  cn.MncName as \"MNC Name\", count(StatusTo) AS \"OPN Ticket\" FROM Complaint C inner join Centre cn On C.MncCode=cn.MNCCODE " +
                          " inner join Department D on CN.DEPTID=D.DEPTID  " +

                          " WHERE  ComplaintDateTime is not null AND trim(ComplaintDateTime) !='0' and  trim(StatusTo)='OPN' and  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') BETWEEN To_Date('" + _dateFrom + "','yyyyMMddHH24MIss') AND To_Date('" + _dateTo + "','yyyyMMddHH24MIss')  " + 
                          " group by  cn.MncName " +
                          "order by  cn.MncName";
                        }
                   
                }




                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        //public DataTable GetOPNHoulySummaryReport(string dateFrom, string dateTo)
        //{
        //    //Creating object of DAL class
        //    CommandData _commnadData = new CommandData();

        //    try
        //    {
        //        _commnadData._CommandType = CommandType.Text;
        //        _commnadData.CommandText = "select * from (select 	Distinct dts.FEEDERNAME, ord.OUTAGE   from OUTAGERECORD ord LEFT JOIN DTSDETAILS dts ON dts.FEEDERID = ord.FEEDERID   where   FEEDERNAME IS NOT NULL AND To_Date(To_Char(DateTime,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') between To_Date('" + dateFrom + "','yyyyMMddHH24MIss') AND  To_Date('" + dateTo + "','yyyyMMddHH24MIss')  AND Trim(Outage) in  ('FAULT','SHUTDOWN') Group BY ord.FEEDERID,ord.OUTAGE, dts.FEEDERNAME ) PIVOT ( COUNT(Outage)  FOR Outage IN ('FAULT' AS FAULT,'SHUTDOWN' AS SHUTDOWN ))";

        //        //Adding Parameters
        //        // _commnadData.AddParameter("@UserName", userID);

        //        //opening connection
        //        _commnadData.OpenWithOutTrans();

        //        //Executing Query
        //        DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

        //        return _ds.Tables[0];
        //    }
        //    catch (Exception ex)
        //    {
        //        //Console.WriteLine("No record found");
        //        throw ex;
        //    }
        //    finally
        //    {
        //        //Console.WriteLine("No ");
        //        _commnadData.Close();

        //    }
        //}
        #endregion

        #region Complaint Handle Per Gang
        public DataTable GetComplaintHandlePerGang(string DateFrom, string DateTo, string Querystring = "", string MncCode = "", string ShiftDateTime="")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (Querystring != "" && Querystring != null)
                {
                    if(MncCode !="" && MncCode !="0")
                    {
                       // _commnadData.CommandText = "SELECT      D.DEPTNAME AS\"IBC / VIBC\", DR.SHIFTNAME SHIFT,   DRT.MTL MTL,    u.UserFullName AS\"Shift Incharge Name\",SE.UserFullName SupervisorElectrical, DRT.GANGNAME as\"Gang Name\",   DR.SHIFTDATE,   DRT.SignInTime,DRT.SignOutTime,C.MNCCODE FROM        DutyRooster     dr INNER   JOIN    DutyRoosterTeam drt  ON      dr.DutyRoosterID    =   drt.DutyRoosterID INNER   JOIN    CENTRE      c    ON      dr.Centre   =   C.MNCNAME INNER   JOIN    Department  d    ON      C.DEPTID    =   D.DEPTID INNER   JOIN    Users   u    ON      dr.ShiftInchargeID  =   u.USerID INNER   JOIN    Users   SE    ON      dr.SupervisorElectricalId  =  SE.USerID left join users M1 on Dr.ComplaintCoordinatorid=M1.USERID  WHERE       To_Date(To_Char(ShiftDate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + "','yyyyMMddHH24MIss') AND      To_Date('" + DateTo + "','yyyyMMddHH24MIss')     AND            DR.SHIFTSIGNINTIME  IS  NOT NULL  and UPPER(trim(DR.SHIFTNAME))='" + Querystring + "' AND trim(C.MNCCODE) in("+MncCode+")  ORDER   BY  DR.SHIFTNAME ASC";
                        if (ShiftDateTime != "")
                        {
                            _commnadData.CommandText = "SELECT      D.DEPTNAME AS\"IBC / VIBC\", DR.SHIFTNAME SHIFT,   DRT.MTL MTL,    u.UserFullName AS\"Shift Incharge Name\",SE.UserFullName SupervisorElectrical, DRT.GANGNAME as\"Gang Name\",   DR.SHIFTDATE,DR.ShiftCreatedDate,   DRT.SignInTime,DRT.SignOutTime,C.MNCCODE FROM        DutyRooster     dr INNER   JOIN    DutyRoosterTeam drt  ON      dr.DutyRoosterID    =   drt.DutyRoosterID INNER   JOIN    CENTRE      c    ON      dr.Centre   =   C.MNCNAME INNER   JOIN    Department  d    ON      C.DEPTID    =   D.DEPTID INNER   JOIN    Users   u    ON      dr.ShiftInchargeID  =   u.USerID INNER   JOIN    Users   SE    ON      dr.SupervisorElectricalId  =  SE.USerID left join users M1 on Dr.ComplaintCoordinatorid=M1.USERID  WHERE       To_Date(To_Char(ShiftCreateddate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') >=  To_Date('" + DateFrom + "','yyyyMMddHH24MIss') AND   To_Date(To_Char(ShiftCreateddate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss')<=    To_Date('" + DateTo + "','yyyyMMddHH24MIss')     AND            DR.SHIFTSIGNINTIME  IS  NOT NULL  and UPPER(trim(DR.SHIFTNAME))='" + Querystring + "' AND trim(C.MNCCODE) in(" + MncCode + ") and " + ShiftDateTime + "  ORDER   BY  DR.SHIFTNAME ASC";

                        }
                        else
                        {
                            _commnadData.CommandText = "SELECT      D.DEPTNAME AS\"IBC / VIBC\", DR.SHIFTNAME SHIFT,   DRT.MTL MTL,    u.UserFullName AS\"Shift Incharge Name\",SE.UserFullName SupervisorElectrical, DRT.GANGNAME as\"Gang Name\",   DR.SHIFTDATE,DR.ShiftCreatedDate ,  DRT.SignInTime,DRT.SignOutTime,C.MNCCODE FROM        DutyRooster     dr INNER   JOIN    DutyRoosterTeam drt  ON      dr.DutyRoosterID    =   drt.DutyRoosterID INNER   JOIN    CENTRE      c    ON      dr.Centre   =   C.MNCNAME INNER   JOIN    Department  d    ON      C.DEPTID    =   D.DEPTID INNER   JOIN    Users   u    ON      dr.ShiftInchargeID  =   u.USerID INNER   JOIN    Users   SE    ON      dr.SupervisorElectricalId  =  SE.USerID left join users M1 on Dr.ComplaintCoordinatorid=M1.USERID  WHERE       To_Date(To_Char(ShiftCreateddate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') >=  To_Date('" + DateFrom + "','yyyyMMddHH24MIss') AND   To_Date(To_Char(ShiftCreateddate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') <=   To_Date('" + DateTo + "','yyyyMMddHH24MIss')     AND            DR.SHIFTSIGNINTIME  IS  NOT NULL  and UPPER(trim(DR.SHIFTNAME))='" + Querystring + "' AND trim(C.MNCCODE) in(" + MncCode + ")  ORDER   BY  DR.SHIFTNAME ASC";

                        }

                    }
                    else
                    {
                        if (ShiftDateTime != "")
                        {
                            _commnadData.CommandText = "SELECT      D.DEPTNAME AS\"IBC / VIBC\", DR.SHIFTNAME SHIFT,   DRT.MTL MTL,    u.UserFullName AS\"Shift Incharge Name\",SE.UserFullName SupervisorElectrical, DRT.GANGNAME as\"Gang Name\",   DR.SHIFTDATE, DR.ShiftCreatedDate,   DRT.SignInTime,DRT.SignOutTime,C.MNCCODE FROM        DutyRooster     dr INNER   JOIN    DutyRoosterTeam drt  ON      dr.DutyRoosterID    =   drt.DutyRoosterID INNER   JOIN    CENTRE      c    ON      dr.Centre   =   C.MNCNAME INNER   JOIN    Department  d    ON      C.DEPTID    =   D.DEPTID INNER   JOIN    Users   u    ON      dr.ShiftInchargeID  =   u.USerID INNER   JOIN    Users   SE    ON      dr.SupervisorElectricalId  =  SE.USerID left join users M1 on Dr.ComplaintCoordinatorid=M1.USERID  WHERE       To_Date(To_Char(ShiftCreateddate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') >=  To_Date('" + DateFrom + "','yyyyMMddHH24MIss') AND    To_Date(To_Char(ShiftCreateddate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss')<=  To_Date('" + DateTo + "','yyyyMMddHH24MIss')     AND            DR.SHIFTSIGNINTIME  IS  NOT NULL  and UPPER(trim(DR.SHIFTNAME))='" + Querystring + "' and " + ShiftDateTime + "  ORDER   BY  DR.SHIFTNAME ASC";

                        }
                        else
                        {
                            _commnadData.CommandText = "SELECT      D.DEPTNAME AS\"IBC / VIBC\", DR.SHIFTNAME SHIFT,   DRT.MTL MTL,    u.UserFullName AS\"Shift Incharge Name\",SE.UserFullName SupervisorElectrical, DRT.GANGNAME as\"Gang Name\",   DR.SHIFTDATE,DR.ShiftCreatedDate,    DRT.SignInTime,DRT.SignOutTime,C.MNCCODE FROM        DutyRooster     dr INNER   JOIN    DutyRoosterTeam drt  ON      dr.DutyRoosterID    =   drt.DutyRoosterID INNER   JOIN    CENTRE      c    ON      dr.Centre   =   C.MNCNAME INNER   JOIN    Department  d    ON      C.DEPTID    =   D.DEPTID INNER   JOIN    Users   u    ON      dr.ShiftInchargeID  =   u.USerID INNER   JOIN    Users   SE    ON      dr.SupervisorElectricalId  =  SE.USerID left join users M1 on Dr.ComplaintCoordinatorid=M1.USERID  WHERE       To_Date(To_Char(ShiftCreateddate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') >=  To_Date('" + DateFrom + "','yyyyMMddHH24MIss') AND   To_Date(To_Char(ShiftCreateddate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') <=   To_Date('" + DateTo + "','yyyyMMddHH24MIss')     AND            DR.SHIFTSIGNINTIME  IS  NOT NULL  and UPPER(trim(DR.SHIFTNAME))='" + Querystring + "'  ORDER   BY  DR.SHIFTNAME ASC";

                        }

                    }


                }
                else
                {
                    if (MncCode != "" && MncCode != "0")
                    {
                        if (ShiftDateTime != "")
                        {
                            _commnadData.CommandText = "SELECT     D.DEPTNAME AS\"IBC / VIBC\", DR.SHIFTNAME SHIFT,   DRT.MTL MTL,    u.UserFullName AS\"Shift Incharge Name\",SE.UserFullName SupervisorElectrical, DRT.GANGNAME as\"Gang Name\",    DR.SHIFTDATE,DR.ShiftCreatedDate,    DRT.SignInTime,DRT.SignOutTime,C.MNCCODE FROM        DutyRooster     dr INNER   JOIN    DutyRoosterTeam drt  ON      dr.DutyRoosterID    =   drt.DutyRoosterID INNER   JOIN    CENTRE      c    ON      dr.Centre   =   C.MNCNAME INNER   JOIN    Department  d    ON      C.DEPTID    =   D.DEPTID INNER   JOIN    Users   u    ON      dr.ShiftInchargeID  =   u.USerID INNER   JOIN    Users   SE    ON      dr.SupervisorElectricalId  =  SE.USerID  WHERE       To_Date(To_Char(ShiftCreateddate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') >=  To_Date('" + DateFrom + "','yyyyMMddHH24MIss') AND   To_Date(To_Char(ShiftCreateddate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss')<=   To_Date('" + DateTo + "','yyyyMMddHH24MIss')     AND            DR.SHIFTSIGNINTIME  IS  NOT NULL AND trim(C.MNCCODE) in(" + MncCode + ") and " + ShiftDateTime + " ORDER   BY    DR.SHIFTNAME  ASC";

                        }
                        else
                        {
                            _commnadData.CommandText = "SELECT     D.DEPTNAME AS\"IBC / VIBC\", DR.SHIFTNAME SHIFT,   DRT.MTL MTL,    u.UserFullName AS\"Shift Incharge Name\",SE.UserFullName SupervisorElectrical, DRT.GANGNAME as\"Gang Name\",    DR.SHIFTDATE,DR.ShiftCreatedDate,    DRT.SignInTime,DRT.SignOutTime,C.MNCCODE FROM        DutyRooster     dr INNER   JOIN    DutyRoosterTeam drt  ON      dr.DutyRoosterID    =   drt.DutyRoosterID INNER   JOIN    CENTRE      c    ON      dr.Centre   =   C.MNCNAME INNER   JOIN    Department  d    ON      C.DEPTID    =   D.DEPTID INNER   JOIN    Users   u    ON      dr.ShiftInchargeID  =   u.USerID INNER   JOIN    Users   SE    ON      dr.SupervisorElectricalId  =  SE.USerID  WHERE      To_Date(To_Char(ShiftCreateddate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') >=  To_Date('" + DateFrom + "','yyyyMMddHH24MIss') AND   To_Date(To_Char(ShiftCreateddate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss')<=    To_Date('" + DateTo + "','yyyyMMddHH24MIss')     AND            DR.SHIFTSIGNINTIME  IS  NOT NULL AND trim(C.MNCCODE) in(" + MncCode + ") ORDER   BY    DR.SHIFTNAME  ASC";

                        }

                    }
                    else
                    {
                        if (ShiftDateTime != "")
                        {
                            _commnadData.CommandText = "SELECT     D.DEPTNAME AS\"IBC / VIBC\", DR.SHIFTNAME SHIFT,   DRT.MTL MTL,    u.UserFullName AS\"Shift Incharge Name\",SE.UserFullName SupervisorElectrical, DRT.GANGNAME as\"Gang Name\",    DR.SHIFTDATE, DR.ShiftCreatedDate,   DRT.SignInTime,DRT.SignOutTime,C.MNCCODE FROM        DutyRooster     dr INNER   JOIN    DutyRoosterTeam drt  ON      dr.DutyRoosterID    =   drt.DutyRoosterID INNER   JOIN    CENTRE      c    ON      dr.Centre   =   C.MNCNAME INNER   JOIN    Department  d    ON      C.DEPTID    =   D.DEPTID INNER   JOIN    Users   u    ON      dr.ShiftInchargeID  =   u.USerID INNER   JOIN    Users   SE    ON      dr.SupervisorElectricalId  =  SE.USerID  WHERE       To_Date(To_Char(ShiftCreateddate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') >=  To_Date('" + DateFrom + "','yyyyMMddHH24MIss') AND   To_Date(To_Char(ShiftCreateddate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss')<=   To_Date('" + DateTo + "','yyyyMMddHH24MIss')     AND            DR.SHIFTSIGNINTIME  IS  NOT NULL and " + ShiftDateTime + " ORDER   BY    DR.SHIFTNAME  ASC";
                        }
                        else
                        {
                            _commnadData.CommandText = "SELECT     D.DEPTNAME AS\"IBC / VIBC\", DR.SHIFTNAME SHIFT,   DRT.MTL MTL,    u.UserFullName AS\"Shift Incharge Name\",SE.UserFullName SupervisorElectrical, DRT.GANGNAME as\"Gang Name\",    DR.SHIFTDATE,DR.ShiftCreatedDate,    DRT.SignInTime,DRT.SignOutTime,C.MNCCODE FROM        DutyRooster     dr INNER   JOIN    DutyRoosterTeam drt  ON      dr.DutyRoosterID    =   drt.DutyRoosterID INNER   JOIN    CENTRE      c    ON      dr.Centre   =   C.MNCNAME INNER   JOIN    Department  d    ON      C.DEPTID    =   D.DEPTID INNER   JOIN    Users   u    ON      dr.ShiftInchargeID  =   u.USerID INNER   JOIN    Users   SE    ON      dr.SupervisorElectricalId  =  SE.USerID  WHERE       To_Date(To_Char(ShiftCreateddate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') >=  To_Date('" + DateFrom + "','yyyyMMddHH24MIss') AND  To_Date(To_Char(ShiftCreateddate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss')<=    To_Date('" + DateTo + "','yyyyMMddHH24MIss')     AND            DR.SHIFTSIGNINTIME  IS  NOT NULL ORDER   BY    DR.SHIFTNAME  ASC";

                        }
                    }
                }

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }



        public DataTable GetComplaintByDate(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString,string MNCCODE="")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "" && QueryString != null)
                {
                    if (MNCCODE != "" && MNCCODE != "0")
                    {
                        _commnadData.CommandText = "select To_Date(C.Complaint_DateTime,'yyyyMMddHH24MIss') Complaint_DateTime,To_Date(C.Status_DateTime,'yyyyMMddHH24MIss') Status_DateTime,C.* from  complaints_log C where  LineMan_Name is not null and  complaint_datetime is not null AND trim(Complaint_DateTime) !='0' AND To_Date(Complaint_DateTime,'yyyyMMddHH24MIss') >=  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND  To_Date(Complaint_DateTime,'yyyyMMddHH24MIss')<=  To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                        
                             " AND trim(C.Status) in('RJCT','E0005') AND " + QueryString + " AND " + MNCCODE;
                    }
                    else
                    {
                        _commnadData.CommandText = "select To_Date(C.Complaint_DateTime,'yyyyMMddHH24MIss') Complaint_DateTime,To_Date(C.Status_DateTime,'yyyyMMddHH24MIss') Status_DateTime,C.* from  complaints_log C where  LineMan_Name is not null and  complaint_datetime is not null AND trim(Complaint_DateTime) !='0' AND To_Date(Complaint_DateTime,'yyyyMMddHH24MIss') >=  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND   To_Date(Complaint_DateTime,'yyyyMMddHH24MIss') <= To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                        
                              " AND trim(C.Status) in('RJCT','E0005') AND " + QueryString;
                    }
                }
                else
                {
                    if (MNCCODE != "" && MNCCODE != "0")
                    {
                        _commnadData.CommandText = "select To_Date(C.Complaint_DateTime,'yyyyMMddHH24MIss') Complaint_DateTime,To_Date(C.Status_DateTime,'yyyyMMddHH24MIss') Status_DateTime,C.* from  complaints_log C where LineMan_Name is not null and  complaint_datetime is not null AND trim(Complaint_DateTime) !='0' AND To_Date(Complaint_DateTime,'yyyyMMddHH24MIss') >=  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND  To_Date(Complaint_DateTime,'yyyyMMddHH24MIss') <=  To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                           " AND trim(C.Status) in('RJCT','E0005') AND "+MNCCODE;
                    }
                    else
                    {
                        _commnadData.CommandText = "select To_Date(C.Complaint_DateTime,'yyyyMMddHH24MIss') Complaint_DateTime,To_Date(C.Status_DateTime,'yyyyMMddHH24MIss') Status_DateTime,C.* from  complaints_log C where LineMan_Name is not null and  complaint_datetime is not null AND trim(Complaint_DateTime) !='0' AND To_Date(Complaint_DateTime,'yyyyMMddHH24MIss') >=  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND   To_Date(Complaint_DateTime,'yyyyMMddHH24MIss') <= To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                         "  AND trim(C.Status) in('RJCT','E0005') ";
                    }

                }

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        public DataTable GetWireBrokenComplaintByDate(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString, string MNCCODE = "")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "" && QueryString != null)
                {
                    if (MNCCODE != "" && MNCCODE != "0")
                    {
                        _commnadData.CommandText = "select To_Date(C.Complaint_DateTime,'yyyyMMddHH24MIss') Complaint_DateTime,To_Date(C.Status_DateTime,'yyyyMMddHH24MIss') Status_DateTime,C.* from  complaints_log C where  LineMan_Name is not null and  complaint_datetime is not null AND trim(Complaint_DateTime) !='0' AND To_Date(Complaint_DateTime,'yyyyMMddHH24MIss') >=  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND  To_Date(Complaint_DateTime,'yyyyMMddHH24MIss')<=  To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +

                             " AND trim(C.Status) in('RJCT','E0005')  AND ParentTicket_NO =0" +
                             " AND upper(trim(Status_Reason)) in ('SHIFT SIGN OFF','SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED','F.I.R CASE - MISSED WIRE PULLED') AND " + QueryString + " AND " + MNCCODE;
                    }
                    else
                    {
                        _commnadData.CommandText = "select To_Date(C.Complaint_DateTime,'yyyyMMddHH24MIss') Complaint_DateTime,To_Date(C.Status_DateTime,'yyyyMMddHH24MIss') Status_DateTime,C.* from  complaints_log C where  LineMan_Name is not null and  complaint_datetime is not null AND trim(Complaint_DateTime) !='0' AND To_Date(Complaint_DateTime,'yyyyMMddHH24MIss') >=  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND   To_Date(Complaint_DateTime,'yyyyMMddHH24MIss') <= To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +

                              " AND ParentTicket_NO =0 AND trim(C.Status) in('RJCT','E0005') AND upper(trim(Status_Reason)) in ('SHIFT SIGN OFF','SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED','F.I.R CASE - MISSED WIRE PULLED') AND " + QueryString;
                    }
                }
                else
                {
                    if (MNCCODE != "" && MNCCODE != "0")
                    {
                        _commnadData.CommandText = "select To_Date(C.Complaint_DateTime,'yyyyMMddHH24MIss') Complaint_DateTime,To_Date(C.Status_DateTime,'yyyyMMddHH24MIss') Status_DateTime,C.* from  complaints_log C where LineMan_Name is not null and  complaint_datetime is not null AND trim(Complaint_DateTime) !='0' AND To_Date(Complaint_DateTime,'yyyyMMddHH24MIss') >=  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND  To_Date(Complaint_DateTime,'yyyyMMddHH24MIss') <=  To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                           " AND ParentTicket_NO =0 AND trim(C.Status) in('RJCT','E0005') AND  upper(trim(Status_Reason)) in ('SHIFT SIGN OFF','SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED','F.I.R CASE - MISSED WIRE PULLED') AND " + MNCCODE;
                    }
                    else
                    {
                        _commnadData.CommandText = "select To_Date(C.Complaint_DateTime,'yyyyMMddHH24MIss') Complaint_DateTime,To_Date(C.Status_DateTime,'yyyyMMddHH24MIss') Status_DateTime,C.* from  complaints_log C where LineMan_Name is not null and  complaint_datetime is not null AND trim(Complaint_DateTime) !='0' AND To_Date(Complaint_DateTime,'yyyyMMddHH24MIss') >=  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND   To_Date(Complaint_DateTime,'yyyyMMddHH24MIss') <= To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                         " AND ParentTicket_NO =0  AND trim(C.Status) in('RJCT','E0005') AND upper(trim(Status_Reason)) in ('SHIFT SIGN OFF','SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED','F.I.R CASE - MISSED WIRE PULLED') ";
                    }

                }

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }
        #endregion

        #region MyRegion Get MOnthly Wire Broken Cable Fault


        public DataTable getMOnthlyWireBrokenCableFault(string MonthYearFrom = "", string MonthYearTo = "", string TimeFrom = "", string TimeTo = "", string QueryString = "")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = "Select DEPTNAME, SUM(TOtalComplaint) NosOfComplaint,SUM(LTCount) LtWirebroken,SUM(HTCount) HtWirebroken,SUM(LTCableCount) LtCableFault from " +
                        "     (select D.deptName , Count(ComplaintDateTime) ToTalComplaint,Sum('0') LTCount,Sum('0') HTCount ,Sum('0') LTCableCount  from Complaint  C inner join Centre cn On C.MncCode=cn.MNCCODE inner join Department D on CN.DEPTID=D.DEPTID where " +
                        //  "   ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //                       "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +
                        "    To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between To_Date('" + TimeFrom + "','yyyyMMddHH24MIss') AND To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " + " AND   " + QueryString + " group by deptName " +

                        "      UNION  " +

                        "     select D.deptName ,Sum('0') ToTalComplaint, Count( ComplaintDateTime) LTCount,Sum('0') HTCount ,Sum('0') LTCableCount  from Complaint  C inner join Centre cn On C.MncCode=cn.MNCCODE inner join Department D on CN.DEPTID=D.DEPTID where  " +
                        //   "   ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //      "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +
                        "    StatusReason is not null AND ((UPPER(TRIM(StatusReason))) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR  (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')) AND  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between To_Date('" + TimeFrom + "','yyyyMMddHH24MIss') AND To_Date('" + TimeTo + "','yyyyMMddHH24MIss')  AND " + QueryString + " group by deptName " +

                        "     UNION  " +
                        "     select D.deptName ,Sum('0') ToTalComplaint,Sum('0') LTCount, Count( ComplaintDateTime) HTCount,Sum('0') LTCableCount  from Complaint  C inner join Centre cn On C.MncCode=cn.MNCCODE inner join Department D on CN.DEPTID=D.DEPTID where   " +
                        //  "   ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //              "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +
                        "   StatusReason is not null AND (UPPER(TRIM(StatusReason))) =    'HT WIRE BROKEN/REPAIRED' AND To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between To_Date('" + TimeTo + "','yyyyMMddHH24MIss') AND To_Date('" + TimeTo + "','yyyyMMddHH24MIss')  AND " + QueryString + " group by deptName" +
                         "     UNION  " +
                        "     select D.deptName ,Sum('0') ToTalComplaint,Sum('0') LTCount, Sum('0') HTCount, Count( ComplaintDateTime) LTCableCount  from Complaint  C inner join Centre cn On C.MncCode=cn.MNCCODE inner join Department D on CN.DEPTID=D.DEPTID where   " +
                        //  "   ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        // "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +
                        "   StatusReason is not null AND UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED' AND To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between To_Date('" + TimeFrom + "','yyyyMMddHH24MIss') AND To_Date('" + TimeTo + "','yyyyMMddHH24MIss')  AND " + QueryString + " group by deptName" +
                        "       ) " +
                        " group by deptName";
                }
                else
                {
                    _commnadData.CommandText = "Select DEPTNAME, SUM(TOtalComplaint) NosOfComplaint,SUM(LTCount) LtWirebroken,SUM(HTCount) HtWirebroken,SUM(LTCableCount) LtCableFault from " +
                                          "     (select D.deptName , Count(ComplaintDateTime) ToTalComplaint,Sum('0') LTCount,Sum('0') HTCount ,Sum('0') LTCableCount  from Complaint  C inner join Centre cn On C.MncCode=cn.MNCCODE inner join Department D on CN.DEPTID=D.DEPTID where " +

                                              "   ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                                  "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +
                                          "  AND To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between To_Date('" + TimeFrom + "','yyyyMMddHH24MIss') AND To_Date('" + TimeTo + "','yyyyMMddHH24MIss') group by deptName " +

                                          "      UNION  " +

                                          "     select D.deptName ,Sum('0') ToTalComplaint, Count( ComplaintDateTime) LTCount,Sum('0') HTCount ,Sum('0') LTCableCount  from Complaint  C inner join Centre cn On C.MncCode=cn.MNCCODE inner join Department D on CN.DEPTID=D.DEPTID where " +
                                          "   ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                                  "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +
                                          " AND   StatusReason is not null AND ((UPPER(TRIM(StatusReason))) IN('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')     OR  (StatusReason LIKE   'F.I.R Case - Missed Wire Pulled%')) AND  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between To_Date('" + TimeFrom + "','yyyyMMddHH24MIss') AND To_Date('" + TimeTo + "','yyyyMMddHH24MIss') group by deptName " +

                                          "     UNION  " +
                                          "     select D.deptName ,Sum('0') ToTalComplaint,Sum('0') LTCount, Count( ComplaintDateTime) HTCount,Sum('0') LTCableCount  from Complaint  C inner join Centre cn On C.MncCode=cn.MNCCODE inner join Department D on CN.DEPTID=D.DEPTID where " +
                                          "   ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                                  "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +
                                          " AND  StatusReason is not null AND (UPPER(TRIM(StatusReason))) =    'HT WIRE BROKEN/REPAIRED' AND To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between To_Date('" + TimeFrom + "','yyyyMMddHH24MIss') AND To_Date('" + TimeTo + "','yyyyMMddHH24MIss') group by deptName" +
                                           "     UNION  " +
                                          "     select D.deptName ,Sum('0') ToTalComplaint,Sum('0') LTCount, Sum('0') HTCount, Count( ComplaintDateTime) LTCableCount  from Complaint  C inner join Centre cn On C.MncCode=cn.MNCCODE inner join Department D on CN.DEPTID=D.DEPTID where  " +
                                          "   ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                                  "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  )" +
                                          " AND StatusReason is not null AND UPPER(TRIM(StatusReason)) =    'LT CABLE FAULT / REPAIRED' AND To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between To_Date('" + TimeFrom + "','yyyyMMddHH24MIss') AND To_Date('" + TimeTo + "','yyyyMMddHH24MIss') group by deptName" +
                                          "       ) " +
                                          " group by deptName";
                }

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }
        #endregion

        #region Lt Breaker



        public DataTable getLtBreakerFault(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString = "")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = " SELECT   DEPTNAME, CRMserviceticket_No, pmt_name,DTS.TRAFOKVA,DTS.BREAKERAMP,MAX(To_Date(STATUS_DATETIME,'yyyyMMddHH24MIss')) Faulty FROM COMPLAINTS_LOG C inner join Centre cn On C.MncCode=cn.MNCCODE inner join Department D on CN.DEPTID=D.DEPTID INNER join DTSDETAILS DTS on UPPER(trim(C.PMT_NAME))=UPPER(trim(DTS.NAMEOFPMTANDSUBSTATIONS)) " +
                                                                 " WHERE TRIM(STATUS_TO)='SSM' AND STATUS='E0007' " +
                        //  "  AND ( To_Char(To_Date(STATUS_DATETIME,'yyyyMMddHH24MIss'),'HH24:MI:SS') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24:MI:SS') " +
                        // "    AND  To_Char(To_Date(STATUS_DATETIME,'yyyyMMddHH24MIss'),'HH24:MI:SS') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24:MI:SS')  )" +
                                                                 " AND " + QueryString + " AND " +
                                                                 " (UPPER(TRIM(STATUS_REASON)))='BREAKER FAULTY' AND " +
                                                                 "To_Date(STATUS_DATETIME,'yyyyMMddHH24MIss') BETWEEN To_Date('" + TimeFrom + "','yyyyMMddHH24MIss') AND To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +

                                                                 " group by DEPTNAME, CRMserviceticket_No, pmt_name,DTS.TRAFOKVA,DTS.BREAKERAMP order by Faulty ASC ";

                }
                else
                {
                    _commnadData.CommandText = " SELECT  DEPTNAME, CRMserviceticket_No, pmt_name,DTS.TRAFOKVA,DTS.BREAKERAMP,MAX(To_Date(STATUS_DATETIME,'yyyyMMddHH24MIss')) Faulty FROM COMPLAINTS_LOG C inner join Centre cn On C.MncCode=cn.MNCCODE inner join Department D on CN.DEPTID=D.DEPTID INNER join DTSDETAILS DTS on UPPER(trim(C.PMT_NAME))=UPPER(trim(DTS.NAMEOFPMTANDSUBSTATIONS)) " +
                                                " WHERE TRIM(STATUS_TO)='SSM' AND STATUS='E0007' " +
                                                  "  AND ( To_Char(To_Date(STATUS_DATETIME,'yyyyMMddHH24MIss'),'HH24:MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24:MI') " +
                                                  "    AND  To_Char(To_Date(STATUS_DATETIME,'yyyyMMddHH24MIss'),'HH24:MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24:MI')  )" +
                                                " AND " +
                                                " (UPPER(TRIM(STATUS_REASON)))='BREAKER FAULTY' AND " +
                                                "To_Date(STATUS_DATETIME,'yyyyMMddHH24MIss') BETWEEN To_Date('" + TimeFrom + "','yyyyMMddHH24MIss') AND To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +

                                                " group by DEPTNAME, CRMserviceticket_No, pmt_name,DTS.TRAFOKVA,DTS.BREAKERAMP order by Faulty ASC ";

                }
                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        public DataTable getLtBreakerReplaced(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString = "")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = " SELECT   DEPTNAME, CRMserviceticket_No, pmt_name,DTS.TRAFOKVA,DTS.BREAKERAMP,MAX(To_Date(STATUS_DATETIME,'yyyyMMddHH24MIss')) Faulty,Internal_Node FROM COMPLAINTS_LOG C inner join Centre cn On C.MncCode=cn.MNCCODE inner join Department D on CN.DEPTID=D.DEPTID INNER join DTSDETAILS DTS on UPPER(trim(C.PMT_NAME))=UPPER(trim(DTS.NAMEOFPMTANDSUBSTATIONS)) " +
                        " WHERE TRIM(STATUS_TO)='SSM' AND STATUS='E0005' " +
                        // "  AND ( To_Char(To_Date(STATUS_DATETIME,'yyyyMMddHH24MIss'),'HH24:MI:SS') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24:MI:SS') " +
                        //  "    AND  To_Char(To_Date(STATUS_DATETIME,'yyyyMMddHH24MIss'),'HH24:MI:SS') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24:MI:SS')  )" +
                                                " AND " + QueryString + " AND " +
                                                " (UPPER(TRIM(STATUS_REASON)))='BREAKER FAULTY' AND " +
                                                "To_Date(STATUS_DATETIME,'yyyyMMddHH24MIss') BETWEEN To_Date('" + TimeFrom + "','yyyyMMddHH24MIss') AND To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +

                                                " Group by  DEPTNAME, CRMserviceticket_No, pmt_name,Internal_Node,DTS.TRAFOKVA,DTS.BREAKERAMP order by Faulty ASC";
                }
                else
                {


                    _commnadData.CommandText = " SELECT   DEPTNAME, CRMserviceticket_No, pmt_name,DTS.TRAFOKVA,DTS.BREAKERAMP,MAX(To_Date(STATUS_DATETIME,'yyyyMMddHH24MIss')) Faulty,Internal_Node FROM COMPLAINTS_LOG C inner join Centre cn On C.MncCode=cn.MNCCODE inner join Department D on CN.DEPTID=D.DEPTID INNER join DTSDETAILS DTS on UPPER(trim(C.PMT_NAME))=UPPER(trim(DTS.NAMEOFPMTANDSUBSTATIONS)) " +
                                                " WHERE TRIM(STATUS_TO)='SSM' AND STATUS='E0005' " +
                                                 "  AND ( To_Char(To_Date(STATUS_DATETIME,'yyyyMMddHH24MIss'),'HH24:MI:SS') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24:MI:SS') " +
                                                "    AND  To_Char(To_Date(STATUS_DATETIME,'yyyyMMddHH24MIss'),'HH24:MI:SS') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24:MI:SS')  )" +

                                                "   AND " +
                                                " (UPPER(TRIM(STATUS_REASON)))='BREAKER FAULTY' AND " +
                                                "To_Date(STATUS_DATETIME,'yyyyMMddHH24MIss') BETWEEN To_Date('" + TimeFrom + "','yyyyMMddHH24MIss') AND To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +

                                                " Group by  DEPTNAME, CRMserviceticket_No, pmt_name,Internal_Node,DTS.TRAFOKVA,DTS.BREAKERAMP order by Faulty ASC";
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }
        #endregion

        #region General Method

        public DataTable getShiftIncharge()
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "select distinct U.UserFullName from dutyrooster D inner join Users U on D.ShiftInchargeid=u.userid";


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable getLineManName(string Department="")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (Department != "" && Department != null)
                {
                    _commnadData.CommandText = "select distinct GangName from DutyRoosterTeam DT inner join DutyRooster D on  DT.DutyRoosterId=D.DutyroosterId  inner join Centre C on D.Centre=C.MNCName AND lower(trim(C.MNCCODE)) in (" + Department + ") order by GangName asc";

                }
                else
                {
                   _commnadData.CommandText = "select  distinct GangName from DutyRoosterTeam order by GangName asc ";

                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetMTL(string Department="")
        {

            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
               // _commnadData.CommandText = "select distinct MTL from Dutyroosterteam order by MTL ASC";
                if (Department != "" && Department !=null)
                    _commnadData.CommandText = "select vehicles.RegNo MTL from vehicles INNER join Department  on vehicles.DEPTID = Department.DEPTID where  lower(trim(Department.DeptName))='" + Department.ToString().Trim().ToLower() + "'order by  RegNo ASC";
                else
                    _commnadData.CommandText = "select distinct MTL from Dutyroosterteam order by MTL ASC";


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable getCenter(string DepartmentName="")
        {
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "select  C.MNCCODE  MNCCODE,C.MNCNAME from Department D inner join Centre C  on D.DeptId=C.DeptId where Upper(trim(D.DeptName))='" +DepartmentName.ToString().Trim().ToUpper() + "'";


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable getStatusTo()
        {
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "select distinct ToWhom from StatusTo ";


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }
        #endregion

        #region Scord Card Method....
        public DataTable getTotalFaultCount(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = "select  Count(*) CMCOUNT from Complaint " +
                                              "  where trim(ComplaintDateTime) !='0' and ComplaintDateTime is not null and " +
                                                 "    TO_DATE(ComplaintDateTime,'yyyyMMddHH24MIss')  " +
                                                 "   Between   TO_DATE('" + TimeFrom + "','yyyyMMddHH24MIss')  AND TO_DATE('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                                 "   AND  " + QueryString;

                    //"  ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                    //"    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                    // "   ) AND " + QueryString + " ";
                }
                else
                {
                    _commnadData.CommandText = "select  Count(*) CMCOUNT from Complaint " +
                    "  where trim(ComplaintDateTime) !='0' and ComplaintDateTime is not null and " +
                    "    TO_DATE(ComplaintDateTime,'yyyyMMddHH24MIss')  " +
                    "   Between   TO_DATE('" + TimeFrom + "','yyyyMMddHH24MIss')  AND TO_DATE('" + TimeTo + "','yyyyMMddHH24MIss') " +
                    "   AND  " +

                     "  ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                     "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                      "   ) ";
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable getFaultByYear(string Year, string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = "select  Count(*) CMCOUNT from Complaint " +
                                              "  where " +
                                                 "   To_char(TO_DATE(ComplaintDateTime,'yyyyMMddHH24MIss'),'YYYY')='" + Year + "'  " +

                                                 " AND " + QueryString;
                    //  "  ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom+TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                    //"    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateFrom +TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                    // "   ) " +
                    //"   AND  " + QueryString;
                }
                else
                {
                    _commnadData.CommandText = "select  Count(*) CMCOUNT from Complaint " +
                    "  where " +
                    "    To_char(TO_DATE(ComplaintDateTime,'yyyyMMddHH24MIss'),'YYYY')='" + Year + "' " +
                      " AND " +
                         "  ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                             "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                              "   ) ";


                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetTotalFaultsOfABCPMT(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = "select  Count(*) CMCOUNT from Complaint  C inner join DTSDETAILS D on Trim(C.PMTNAME)=Trim(D.TrafoName)" +
                                              "  where  trim(D.ABC)='T' AND trim(ComplaintDateTime) !='0' and ComplaintDateTime is not null and " +
                                                 "    TO_DATE(ComplaintDateTime,'yyyyMMddHH24MIss')  " +
                                                 "   Between   TO_DATE('" + TimeFrom + "','yyyyMMddHH24MIss')  AND TO_DATE('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                                 "   AND  " + QueryString;

                    //"  ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                    //"    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                    // "   ) AND " + QueryString + " ";
                }
                else
                {
                    _commnadData.CommandText = "select  Count(*) CMCOUNT from Complaint C inner join DTSDETAILS D on Trim(C.PMTNAME)=Trim(D.TrafoName) " +
                    "  where  trim(D.ABC)='T' AND trim(ComplaintDateTime) !='0' and ComplaintDateTime is not null and " +
                    "    TO_DATE(ComplaintDateTime,'yyyyMMddHH24MIss')  " +
                    "   Between   TO_DATE('" + TimeFrom + "','yyyyMMddHH24MIss')  AND TO_DATE('" + DateTo + "','yyyyMMddHH24MIss') " +
                    "   AND  " +

                     "  ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                     "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                      "   ) ";
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable getTotalFaultsOfABCPMTByYear(string Year, string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = "select  Count(*) CMCOUNT from Complaint C inner join DTSDETAILS D on Trim(C.PMTNAME)=Trim(D.TrafoName) " +
                                              "  where TRIM(D.ABC)='T' AND   trim(ComplaintDateTime) !='0' and ComplaintDateTime is not null and   " +

                                              "    TO_DATE(ComplaintDateTime,'yyyyMMddHH24MIss')  " +
                                                  "   Between   TO_DATE('" + TimeFrom + "','yyyyMMddHH24MIss')  AND TO_DATE('" + TimeTo + "','yyyyMMddHH24MIss') AND " +
                                                 "   To_char(TO_DATE(ComplaintDateTime,'yyyyMMddHH24MIss'),'YYYY')='" + Year + "'  " +

                                                 " AND " + QueryString;
                    //  "  ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                    //"    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                    // "   ) " +
                    //"   AND  " + QueryString;
                }
                else
                {
                    _commnadData.CommandText = "select  Count(*) CMCOUNT from Complaint  C inner join DTSDETAILS D on Trim(C.PMTNAME)=Trim(D.TrafoName) " +
                    "  where TRIM(D.ABC)='T' AND   trim(ComplaintDateTime) !='0' and ComplaintDateTime is not null and  " +
                      "    TO_DATE(ComplaintDateTime,'yyyyMMddHH24MIss')  " +
                                                  "   Between   TO_DATE('" + TimeFrom + "','yyyyMMddHH24MIss')  AND TO_DATE('" + TimeTo + "','yyyyMMddHH24MIss') AND " +
                    "    To_char(TO_DATE(ComplaintDateTime,'yyyyMMddHH24MIss'),'YYYY')='" + Year + "' " +
                      " AND " +
                         "  ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                             "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                              "   ) ";


                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetTotalABCExectuted(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = "select  Count(*) CMCOUNT from Complaint  C inner join DTSDETAILS D on Trim(C.PMTNAME)=Trim(D.TrafoName)" +
                                              "  where  trim(D.ABC)='T' AND trim(ComplaintDateTime) !='0' and ComplaintDateTime is not null and " +
                                                 "    TO_DATE(ComplaintDateTime,'yyyyMMddHH24MIss')  " +
                                                 "   Between   TO_DATE('" + TimeFrom + "','yyyyMMddHH24MIss')  AND TO_DATE('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                                 "     " +
                                                 " AND ( D.ABCDate is not null OR D.ABCDATE !=null ) AND  " +
                                                 "  (TO_CHAR(TO_DATE(ABCDATE,'DD/MM/YY'), 'yyyyMMdd')	>=  TO_CHAR(TO_DATE('" + TimeFrom + "','yyyyMMddHH24MIss'), 'yyyyMMdd') AND " +
                                                 "   TO_CHAR(TO_DATE(ABCDATE,'DD/MM/YY'), 'yyyyMMdd')	<=  TO_CHAR(TO_DATE('" + TimeTo + "','yyyyMMddHH24MIss'), 'yyyyMMdd') ) AND " + QueryString;

                    //"  ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                    //"    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                    // "   ) AND " + QueryString + " ";
                }
                else
                {
                    _commnadData.CommandText = "select  Count(*) CMCOUNT from Complaint  C inner join DTSDETAILS D on Trim(C.PMTNAME)=Trim(D.TrafoName)" +
                                               "  where  trim(D.ABC)='T' AND trim(ComplaintDateTime) !='0' and ComplaintDateTime is not null and " +
                                                  "    TO_DATE(ComplaintDateTime,'yyyyMMddHH24MIss')  " +
                                                  "   Between   TO_DATE('" + TimeFrom + "','yyyyMMddHH24MIss')  AND TO_DATE('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                                  "   AND  " +
                                                  "  ( D.ABCDate is not null OR D.ABCDATE !=null ) AND  " +
                                                  "  (TO_CHAR(TO_DATE(ABCDATE,'DD/MM/YY'), 'yyyyMMdd')	>=  TO_CHAR(TO_DATE('" + TimeFrom + "','yyyyMMddHH24MIss'), 'yyyyMMdd') AND " +
                                                  "   TO_CHAR(TO_DATE(ABCDATE,'DD/MM/YY'), 'yyyyMMdd')	<=  TO_CHAR(TO_DATE('" + TimeTo + "','yyyyMMddHH24MIss'), 'yyyyMMdd')) AND " +

                                                   "  ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                                   "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                                    "   )  ";
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetTotalABCExectutedByYear(string Year, string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = "select  Count(*) CMCOUNT from Complaint C inner join DTSDETAILS D on Trim(C.PMTNAME)=Trim(D.TrafoName) " +
                                              "  where TRIM(D.ABC)='T' AND  trim(ComplaintDateTime) !='0' and ComplaintDateTime is not null and " +

                                                 "    TO_DATE(ComplaintDateTime,'yyyyMMddHH24MIss')  " +
                                                  "   Between   TO_DATE('" + TimeFrom + "','yyyyMMddHH24MIss')  AND TO_DATE('" + TimeTo + "','yyyyMMddHH24MIss') AND " +
                                                 "   To_char(TO_DATE(ComplaintDateTime,'yyyyMMddHH24MIss'),'YYYY')='" + Year + "'  " +

                                                 "  " +
                                                  " AND ( D.ABCDate is not null OR D.ABCDATE !=null ) AND  " +
                                                 "  TO_CHAR(TO_DATE(ABCDATE,'MM/DD/YY'), 'yyyy')='" + Year + "' AND " + QueryString;

                    //  "  ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                    //"    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                    // "   ) " +
                    //"   AND  " + QueryString;
                }
                else
                {
                    _commnadData.CommandText = "select  Count(*) CMCOUNT from Complaint  C inner join DTSDETAILS D on Trim(C.PMTNAME)=Trim(D.TrafoName) " +
                    "  where TRIM(D.ABC)='T' AND trim(ComplaintDateTime) !='0' and ComplaintDateTime is not null and " +
                        "    TO_DATE(ComplaintDateTime,'yyyyMMddHH24MIss')  " +
                                                  "   Between   TO_DATE('" + TimeFrom + "','yyyyMMddHH24MIss')  AND TO_DATE('" + TimeTo + "','yyyyMMddHH24MIss') AND " +
                    "    To_char(TO_DATE(ComplaintDateTime,'yyyyMMddHH24MIss'),'YYYY')='" + Year + "' " +
                      "  " +
                       " AND ( D.ABCDate is not null OR D.ABCDATE !=null ) AND  " +
                                                 "  TO_CHAR(TO_DATE(ABCDATE,'MM/DD/YY'), 'yyyy')='" + Year + "' AND " +
                         "  ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                             "    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                              "   ) ";


                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        #endregion

        #region LT SAIFI SAIDI..


        public DataTable getLTSAIFISAIDI(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = "select DISTINCT C.pmtname,round(SUM(C.TimeSlice*60)/Count(Complaintid),0) AvgTat,C.DTSID,TO_Char(TO_DATE(C.ComplaintAttemptDateTime,    'yyyyMMddHH24MIss'),'Mon-YYYY') MonthYear,DTS.NOOfConsumer,Count(Complaintid) TotalFault from COmplaint C " +
                        " inner join dtsdetails DTS on  TRIM(C.DTSID)=trim(DTS.DTSID) " +
                    "  where " + QueryString + " AND PmtName is not null AND " +
                    "  TO_DATE(ComplaintAttemptDateTime,    'yyyyMMddHH24MIss')     BETWEEN        TO_DATE('" + TimeFrom + "',    'yyyyMMddHH24MIss')    AND      TO_DATE('" + TimeTo + "',    'yyyyMMddHH24MIss')    AND Status in('E0005','E0006') " +

                     "   AND ( To_Char(To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss'),'HH24:MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24:MI') " +
                     "    AND  To_Char(To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss'),'HH24:MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24:MI') " +
                      "   ) Group by DTS.NOOfConsumer,C.pmtname,C.DTSID,TO_Char(TO_DATE(C.ComplaintAttemptDateTime,    'yyyyMMddHH24MIss'),'Mon-YYYY')  ";
                }
                else
                {
                    _commnadData.CommandText = "select DISTINCT C.pmtname,round(SUM(C.TimeSlice*60)/Count(Complaintid),0) AvgTat,C.DTSID,TO_Char(TO_DATE(C.ComplaintAttemptDateTime,    'yyyyMMddHH24MIss'),'Mon-YYYY') MonthYear,DTS.NOOfConsumer,Count(Complaintid) TotalFault from COmplaint C " +
                        " inner join dtsdetails DTS on  TRIM(C.DTSID)=trim(DTS.DTSID) " +
                    "  where PmtName is not null and  " +
                    "  TO_DATE(ComplaintAttemptDateTime,    'yyyyMMddHH24MIss')     BETWEEN        TO_DATE('" + TimeFrom + "',    'yyyyMMddHH24MIss')    AND      TO_DATE('" + TimeTo + "',    'yyyyMMddHH24MIss')    AND Status in('E0005','E0006') " +

                     "   AND ( To_Char(To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss'),'HH24:MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24:MI') " +
                     "    AND  To_Char(To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss'),'HH24:MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24:MI') " +
                      "   )  Group by DTS.NOOfConsumer,C.pmtname,C.DTSID,TO_Char(TO_DATE(C.ComplaintAttemptDateTime,    'yyyyMMddHH24MIss'),'Mon-YYYY') ";
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable getTotalPMTByIBC(string IBCNAME)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;

                _commnadData.CommandText = "select Count(*) from Dtsdetails where lower(trim(ibc))='" + IBCNAME.Trim().ToLower() + "' ";



                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }
        public DataTable getTotalConsumerByIBC(string IBCNAME)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;

                // _commnadData.CommandText = "select Count(*) from Dtsdetails where lower(trim(ibc))='ibc defence' ";
                _commnadData.CommandText = "select Sum(noofconsumer) from Dtsdetails where lower(trim(ibc))='" + IBCNAME.Trim().ToLower() + "' ";




                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        #endregion

        #region  Consumer Report...
        public DataTable getConsumerMappingDetail(string DateFrom, string DateTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;

                _commnadData.CommandText = " select Complaint.FeederId FeederId,Complaint.FeederName FeederName,Complaint.DTSID DTSID, d.DTS DTSName,Complaint.PMTNAME PMTNAME  from Complaint left join dtsdetails d on Complaint.DTSID=d.DTSID where " + QueryString + " AND   ComplaintDateTime !='0' and ComplaintDateTime is not null and " +
                        " TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss')     BETWEEN        TO_DATE('" + DateFrom + "',    'yyyyMMddHH24MIss')    AND      TO_DATE('" + DateTo + "',    'yyyyMMddHH24MIss') and rownum=1 order by Complaintid desc ";

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        public DataTable getConsumerComplaintRecord(string DateFrom, string DateTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;

                _commnadData.CommandText = " select CRMSERVICETICKETNO, To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') ReceivedTime,To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss') AttemptTime,StatusReason from Complaint where " + QueryString + " AND Status in('E0005','E0006') AND   ComplaintDateTime !='0' and ComplaintDateTime is not null and " +
                    " TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss')     BETWEEN        TO_DATE('" + DateFrom + "',    'yyyyMMddHH24MIss')    AND      TO_DATE('" + DateTo + "',    'yyyyMMddHH24MIss') and rownum <11  order by Complaintid desc ";




                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        public DataTable getPMTHTLTBreakup(string DateFrom, string DateTo, string QueryString, string PmtName)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;

                _commnadData.CommandText = " select StatusTo,Count(*) TotalComplaint from Complaint where " + QueryString + " AND trim(PMTNAME)='" + PmtName + "' AND  ComplaintDateTime !='0' and ComplaintDateTime is not null and " +
                    " TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss')     BETWEEN        TO_DATE('" + DateFrom + "',    'yyyyMMddHH24MIss')    AND      TO_DATE('" + DateTo + "',    'yyyyMMddHH24MIss') AND statusTo is not null  group by StatusTo";




                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }





        public DataTable getFeederIntruption(string DateFrom, string DateTo, string QueryString, string DTSID, string FeederID)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                _commnadData.CommandText = "select * from (select DateTime,Outage  from OUtageRecord  where  ( FeederID='" + DTSID + "' OR FeederID='" + FeederID + "') AND To_Date(To_Char(DateTime,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') between To_Date('" + DateFrom + "','yyyyMMddHH24MIss') AND  To_Date('" + DateTo + "','yyyyMMddHH24MIss')  AND Trim(Outage) in  ('FAULT','SHUTDOWN') Group BY Outage,DateTime ) PIVOT ( COUNT(Outage)  FOR Outage IN ('FAULT' AS FAULT,'SHUTDOWN' AS SHUTDOWN ))";

                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }
        #endregion

        //#region LT Cable Fault
        //public DataTable getLTCableFault(string DateFrom, string DateTo, string QueryString)
        //{
        //    //Creating object of DAL class
        //    CommandData _commnadData = new CommandData();

        //    try
        //    {
        //        _commnadData._CommandType = CommandType.Text;
        //        if (QueryString != "")
        //        {
        //            _commnadData.CommandText = "select  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'dd-MON-yy') ReceivedDate " +
        //                                    "  ,To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24:MI') ReceivedTime,TRIM(Address) Address " +
        //                                    " ,Cn.MncName Centre,To_Char(To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss'),'dd-MON-yy') AttemptDate " +
        //                                    ",To_Char(To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss'),'HH24:MI') AttemptTime " +
        //                                    ", floor((To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss')-To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'))*24)     || ':' ||       mod(floor((To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss')-To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'))*24*60)" +
        //                                    ",60)     Duration  from Complaint C inner join Centre Cn on C.MncCode=Cn.MncCode where " + QueryString + " and To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between To_Date('" + DateFrom + "' " +
        //                                    ",'yyyyMMddHH24MIss')  and To_Date('" + DateTo + "','yyyyMMddHH24MIss') and status in ('E0005','E0006') order by ComplaintDateTime asc  ";

        //        }
        //        else
        //        {
        //            _commnadData.CommandText = "select  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'dd-MON-yy') ReceivedDate " +
        //                                    "  ,To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24:MI') ReceivedTime,TRIM(Address) Address " +
        //                                    " ,Cn.MncName Centre,To_Char(To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss'),'dd-MON-yy') AttemptDate " +
        //                                    ",To_Char(To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss'),'HH24:MI') AttemptTime " +
        //                                    ", floor((To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss')-To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'))*24)     || ':' ||       mod(floor((To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss')-To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'))*24*60)" +
        //                                    ",60)     Duration  from Complaint C inner join Centre Cn on C.MncCode=Cn.MncCode where  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between To_Date('" + DateFrom + "' " +
        //                                    ",'yyyyMMddHH24MIss')  and To_Date('" + DateTo + "','yyyyMMddHH24MIss') and status in ('E0005','E0006') order by ComplaintDateTime asc ";
        //        }


        //        //Adding Parameters
        //        // _commnadData.AddParameter("@UserName", userID);

        //        //opening connection
        //        _commnadData.OpenWithOutTrans();

        //        //Executing Query
        //        DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

        //        return _ds.Tables[0];
        //    }
        //    catch (Exception ex)
        //    {
        //        //Console.WriteLine("No record found");
        //        throw ex;
        //    }
        //    finally
        //    {
        //        //Console.WriteLine("No ");
        //        _commnadData.Close();

        //    }
        //}
        //#endregion

        #region LT Cable Fault
        public DataTable getLTCableFault(string DateFrom, string DateTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = "select  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'dd-MON-yy') ReceivedDate " +
                                            "  ,To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24:MI') ReceivedTime,TRIM(Address) Address " +
                                            " ,Cn.MncName Centre,To_Char(To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss'),'dd-MON-yy') AttemptDate " +
                                            ",To_Char(To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss'),'HH24:MI') AttemptTime " +
                                            ", floor((To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss')-To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'))*24)     || ':' ||       mod(floor((To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss')-To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'))*24*60)" +
                                            ",60)     Duration  from Complaint C inner join Centre Cn on C.MncCode=Cn.MncCode where " + QueryString + " and To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between To_Date('" + DateFrom + "' " +
                                            ",'yyyyMMddHH24MIss')  and To_Date('" + DateTo + "','yyyyMMddHH24MIss') and status in ('E0005','E0006') AND UPPER(STATUSTO) ='UGM' AND UPPER(STATUSREASON) = 'LT CABLE FAULT / REPAIRED' order by ComplaintDateTime asc  ";

                }
                else
                {
                    _commnadData.CommandText = "select  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'dd-MON-yy') ReceivedDate " +
                                            "  ,To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24:MI') ReceivedTime,TRIM(Address) Address " +
                                            " ,Cn.MncName Centre,To_Char(To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss'),'dd-MON-yy') AttemptDate " +
                                            ",To_Char(To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss'),'HH24:MI') AttemptTime " +
                                            ", floor((To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss')-To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'))*24)     || ':' ||       mod(floor((To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss')-To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'))*24*60)" +
                                            ",60)     Duration  from Complaint C inner join Centre Cn on C.MncCode=Cn.MncCode where  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between To_Date('" + DateFrom + "' " +
                                            ",'yyyyMMddHH24MIss')  and To_Date('" + DateTo + "','yyyyMMddHH24MIss') and status in ('E0005','E0006') AND UPPER(STATUSTO) ='UGM' AND UPPER(STATUSREASON) = 'LT CABLE FAULT / REPAIRED' order by ComplaintDateTime asc ";
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }
        #endregion

        #region LineMan Progress Sheet....



        public DataTable getLineManDetail(string DateFrom, string DateTo, string QueryString,string IbcCode)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    if(IbcCode !="" && IbcCode !="0")
                    _commnadData.CommandText = "select  D.Centre,DT.MTL,DT.SignInTime,Dt.SignOutTime,M1.UserFullName Member1,M2.UserFullName Member2,M3.UserFullName Member3,M4.UserFullName Member4,M5.UserFullName Member5,M6.UserFullName Member6, " +
                                            "  M7.UserFullName Member7,SI.UserFullName ShiftIncharge,SE.UserFullName Supervisor,CC.UserFullName ComplaintCoordinator,  D.ShiftName,D.ShiftStartTime,D.ShiftEndTime,        D.ShiftSIgnInTime,D.ShiftDate from DutyRooster D  " +
                                              " inner join Centre Cn on lower(trim(D.Centre))=lower(trim(Cn.MNCNAME)) AND trim(Cn.MNCCODE) in (" + IbcCode + ") " +
                                            " inner join DutyRoosterTeam DT on D.DUtyroosterid=DT.DutyRoosterid        inner join users SI on D.ShiftInchargeid=SI.USERID " +
                                            "inner join users SE on D.SupervisorElectricalId=SE.USERID        inner join users CC on D.ComplaintCoordinatorid=CC.USERID " +
                                            " left join users M1 on DT.Member1=M1.USERID        left join users M2 on DT.Member2=M2.USERID        left join users M3 on DT.Member3=M3.USERID " +
                                            " left join users M4 on DT.Member4=M4.USERID        left join users M5 on DT.Member5=M5.USERID        left join users M6 on DT.Member6=M6.USERID " +
                                            " left join users M7 on DT.Member7=M7.USERID " +

                                            " where shiftsignoutTime is not null and ShiftSignInTime is not null    AND To_Date(To_Char(ShiftDate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + "','yyyyMMddHH24MIss') AND      To_Date('" + DateTo + "','yyyyMMddHH24MIss') and "  + QueryString +
                                            " order by D.Dutyroosterid ASC  ";
                    else
                        _commnadData.CommandText = "select  D.Centre,DT.MTL,DT.SignInTime,Dt.SignOutTime,M1.UserFullName Member1,M2.UserFullName Member2,M3.UserFullName Member3,M4.UserFullName Member4,M5.UserFullName Member5,M6.UserFullName Member6, " +
                                           "  M7.UserFullName Member7,SI.UserFullName ShiftIncharge,SE.UserFullName Supervisor,CC.UserFullName ComplaintCoordinator,  D.ShiftName,D.ShiftStartTime,D.ShiftEndTime,        D.ShiftSIgnInTime,D.ShiftDate from DutyRooster D  " +
                                             
                                           " inner join DutyRoosterTeam DT on D.DUtyroosterid=DT.DutyRoosterid        inner join users SI on D.ShiftInchargeid=SI.USERID " +
                                           "inner join users SE on D.SupervisorElectricalId=SE.USERID        inner join users CC on D.ComplaintCoordinatorid=CC.USERID " +
                                           " left join users M1 on DT.Member1=M1.USERID        left join users M2 on DT.Member2=M2.USERID        left join users M3 on DT.Member3=M3.USERID " +
                                           " left join users M4 on DT.Member4=M4.USERID        left join users M5 on DT.Member5=M5.USERID        left join users M6 on DT.Member6=M6.USERID " +
                                           " left join users M7 on DT.Member7=M7.USERID " +

                                           " where shiftsignoutTime is not null and ShiftSignInTime is not null    AND To_Date(To_Char(ShiftDate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + "','yyyyMMddHH24MIss') AND      To_Date('" + DateTo + "','yyyyMMddHH24MIss') and " + QueryString +
                                           " order by D.Dutyroosterid ASC  ";

                }
                else
                {
                    if (IbcCode != "" && IbcCode != "0")
                    _commnadData.CommandText = "select  D.Centre,DT.MTL ,DT.SignInTime,Dt.SignOutTime,M1.UserFullName Member1,M2.UserFullName Member2,M3.UserFullName Member3,M4.UserFullName Member4,M5.UserFullName Member5,M6.UserFullName Member6, " +
                                            "  M7.UserFullName Member7,SI.UserFullName ShiftIncharge,SE.UserFullName Supervisor,CC.UserFullName ComplaintCoordinator,  D.ShiftName,D.ShiftStartTime,D.ShiftEndTime,        D.ShiftSIgnInTime,D.ShiftDate from DutyRooster D  " +
                                               " inner join Centre Cn on lower(trim(D.Centre))=lower(trim(Cn.MNCNAME)) AND trim(Cn.MNCCODE) in (" + IbcCode + ") " +
                                            " inner join DutyRoosterTeam DT on D.DUtyroosterid=DT.DutyRoosterid        inner join users SI on D.ShiftInchargeid=SI.USERID " +
                                            "inner join users SE on D.SupervisorElectricalId=SE.USERID        inner join users CC on D.ComplaintCoordinatorid=CC.USERID " +
                                            " left join users M1 on DT.Member1=M1.USERID        left join users M2 on DT.Member2=M2.USERID        left join users M3 on DT.Member3=M3.USERID " +
                                            " left join users M4 on DT.Member4=M4.USERID        left join users M5 on DT.Member5=M5.USERID        left join users M6 on DT.Member6=M6.USERID " +
                                            " left join users M7 on DT.Member7=M7.USERID " +

                                            " where shiftsignoutTime is not null and ShiftSignInTime is not null    AND To_Date(To_Char(ShiftDate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + "','yyyyMMddHH24MIss') AND      To_Date('" + DateTo + "','yyyyMMddHH24MIss')  " +
                                            " order by D.Dutyroosterid ASC  ";
                    else
                        _commnadData.CommandText = "select  D.Centre,DT.MTL ,DT.SignInTime,Dt.SignOutTime,M1.UserFullName Member1,M2.UserFullName Member2,M3.UserFullName Member3,M4.UserFullName Member4,M5.UserFullName Member5,M6.UserFullName Member6, " +
                                          "  M7.UserFullName Member7,SI.UserFullName ShiftIncharge,SE.UserFullName Supervisor,CC.UserFullName ComplaintCoordinator,  D.ShiftName,D.ShiftStartTime,D.ShiftEndTime,        D.ShiftSIgnInTime,D.ShiftDate from DutyRooster D  " +
                                          
                                          " inner join DutyRoosterTeam DT on D.DUtyroosterid=DT.DutyRoosterid        inner join users SI on D.ShiftInchargeid=SI.USERID " +
                                          "inner join users SE on D.SupervisorElectricalId=SE.USERID        inner join users CC on D.ComplaintCoordinatorid=CC.USERID " +
                                          " left join users M1 on DT.Member1=M1.USERID        left join users M2 on DT.Member2=M2.USERID        left join users M3 on DT.Member3=M3.USERID " +
                                          " left join users M4 on DT.Member4=M4.USERID        left join users M5 on DT.Member5=M5.USERID        left join users M6 on DT.Member6=M6.USERID " +
                                          " left join users M7 on DT.Member7=M7.USERID " +

                                          " where shiftsignoutTime is not null and ShiftSignInTime is not null    AND To_Date(To_Char(ShiftDate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + "','yyyyMMddHH24MIss') AND      To_Date('" + DateTo + "','yyyyMMddHH24MIss')  " +
                                          " order by D.Dutyroosterid ASC  ";
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        public DataTable GetComplaintBetweenDateRange(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {



                    _commnadData.CommandText = " select   MTL,crmserviceticket_no as \"CRM NO\", flag,Address,To_Date(Accept_DateTime,'yyyyMMddHH24MIss')  AS \"Accept Time\" ,S.StatusName AS Status,To_Date(Status_DateTime,'yyyyMMddHH24MIss') AS \"Complete / Forward Time\" , AcceptCompTat TAT     from COmplaints_log inner join status S on Status=s.StatusCode " +
                                                "Where Accept_DateTime is not null and Accept_DateTime !='NONE' and AcceptByMTL is not null and  AcceptByMTL !='NONE'  and Complaint_DateTime is not null and  trim(Complaint_DateTime) !='0' and  " +
                                                 " To_Date(Complaint_DateTime,'yyyyMMddHH24MIss') between To_Date('" + TimeFrom + "','yyyyMMddHH24MIss') and To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                                  " ANd Status in ('E0005','E0007') and MTL is not NULL and LineMan_Name is not null " +


                                               "  AND " + QueryString + " order by crmserviceticket_no ASC";
                   
                }
                else
                {
                    _commnadData.CommandText = " select   MTL,crmserviceticket_no as \"CRM NO\", flag,Address, To_Date(Accept_DateTime,'yyyyMMddHH24MIss') AS \"Accept Time\" ,S.StatusName Status,To_Date(Status_DateTime,'yyyyMMddHH24MIss') AS \"Complete / Forward Time\" , AcceptCompTat TAT     from COmplaints_log inner join status S on Status=s.StatusCode  " +
                                                                    "Where Accept_DateTime is not null and Accept_DateTime !='NONE' and AcceptByMTL is not null and  AcceptByMTL !='NONE'  and Complaint_DateTime is not null and  trim(Complaint_DateTime) !='0' and  " +
                                                                       " To_Date(Complaint_DateTime,'yyyyMMddHH24MIss') between To_Date('" + TimeFrom + "','yyyyMMddHH24MIss') and To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                                                        " AND ( " +

                                                                         " To_Char(To_Date(Complaint_DateTime,'yyyyMMddHH24MIss'),'HH24.MI') >= To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                                                         " AND " +

                                                                            " To_Char(To_Date(Complaint_DateTime,'yyyyMMddHH24MIss'),'HH24.MI') <= To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +

                                                                            " ) " +
                                                                            " ANd Status in ('E0005','E0007') and MTL is not NULL and LineMan_Name is not null order by crmserviceticket_no ASC";

                  }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        #endregion

        #region Slide2...

        public DataTable getComplaints(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = "select SI,  To_Char(To_Date(ComplaintDateTime,    'yyyyMMddHH24MIss'),'MON-yy') ComplaintDateTime,NVL2(StatusTo,StatusTo,'Test') StatusTo,TO_NUMBER(trim(NVL2(ReOpenCount,ReOpenCount,'0'))) ReOpenCount from Complaint " +
                        " where  trim(ComplaintDateTime) !='0' AND ComplaintDateTime is not  null   " +
                        " AND To_Date(ComplaintDateTime,    'yyyyMMddHH24MIss') between To_Date('" + TimeFrom + "',    'yyyyMMddHH24MIss') AND To_Date('" + TimeTo + "',    'yyyyMMddHH24MIss')  AND  " +
                        // " AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=      To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI')    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI')    <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  ) AND " +
                        QueryString +

                        " order by ComplaintDateTime ASC  ";

                }
                else
                {
                    _commnadData.CommandText = "select  SI, To_Char(To_Date(ComplaintDateTime,    'yyyyMMddHH24MIss'),'MON-yy') ComplaintDateTime,NVL2(StatusTo,StatusTo,'Test') StatusTo,TO_NUMBER(trim(NVL2(ReOpenCount,ReOpenCount,'0'))) ReOpenCount from Complaint " +
                       " where  trim(ComplaintDateTime) !='0' AND ComplaintDateTime is not  null   " +
                       " AND To_Date(ComplaintDateTime,    'yyyyMMddHH24MIss') between To_Date('" + TimeFrom + "',    'yyyyMMddHH24MIss') AND To_Date('" + TimeTo + "',    'yyyyMMddHH24MIss') " +
                        // " AND ( To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=      To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI')    AND  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI')    <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')  ) " +


                       " order by ComplaintDateTime ASC  ";
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        #endregion

        #region Complaint & Wiew Beokwn R-II...



        public DataTable getWireBrokenComplaint(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {



                    string Query = "   Select  *from ( " +
                                  "  SELECT To_Char(TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss'),'MON') MonthOfYear,D.DeptName,cn.MncName ,  StatusREASON " +
                                  "  FROM COMPLAINT C   " +
                                  "  inner join Centre Cn on trim(C.MNCCODE)=trim(Cn.MNCCODE) inner join Department D on Cn.DeptId=D.deptId  WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null " +
                        //"   AND  " +
                        //"  ( " +
                        //" To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //" AND  " +
                        //"  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //"   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss') " +
                                    " ) " +
                                    " AND   " + QueryString + " AND  " +
                                  " ( " +
                                  " UPPER(TRIM(StatusReason)) IN   ('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')       OR    StatusReason like '%F.I.R Case - Missed Wire Pulled%'   ) " +
                                  "  )  " +
                                    " PIVOT  (     Count(StatusREASON)     FOR (MonthOfYear) in ('JAN' AS JAN,'FEB' as FEB,'MAR' AS MAR,'APR' as APRIL,'MAY' as MAY,'JUN' as JUN,'JUL' as JULY,'AUG' as AUG,'SEP' AS SEP,'OCT' as OCT,'NOV' as NOV, 'DEC' AS DEC)         ) ";



                    _commnadData.CommandText = Query;



                }
                else
                {

                    string Query = "   Select  *from ( " +
                                 "  SELECT To_Char(TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss'),'MON') MonthOfYear,D.DeptName,cn.MncName ,  StatusREASON " +
                                 "  FROM COMPLAINT C   " +
                                 "  inner join Centre Cn on trim(C.MNCCODE)=trim(Cn.MNCCODE) inner join Department D on Cn.DeptId=D.deptId  WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null " +
                                 "   AND  " +
                                 "  ( " +
                                 " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                 " AND  " +
                                 "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                 "   ) " +
                                 "  AND  " +
                                 "  ( " +
                                  "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss') " +
                                   " ) " +
                                   " AND  " +
                                 " ( " +
                                 " UPPER(TRIM(StatusReason)) IN   ('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')       OR    StatusReason like '%F.I.R Case - Missed Wire Pulled%'   ) " +
                                 "  )   " +
                                   " PIVOT  (     Count(StatusREASON)     FOR (MonthOfYear) in ('JAN' AS JAN,'FEB' as FEB,'MAR' AS MAR,'APR' as APRIL,'MAY' as MAY,'JUN' as JUN,'JUL' as JULY,'AUG' as AUG,'SEP' AS SEP,'OCT' as OCT,'NOV' as NOV, 'DEC' AS DEC)         ) ";

                    _commnadData.CommandText = Query;
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }



        public DataTable getComplaintReceived(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {



                    string Query = "   Select  *from ( " +
                                  "  SELECT To_Char(TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss'),'MON') MonthOfYear,D.DeptName,cn.MncName ,  ComplaintId " +
                                  "  FROM COMPLAINT C   " +
                                  "  inner join Centre Cn on trim(C.MNCCODE)=trim(Cn.MNCCODE) inner join Department D on Cn.DeptId=D.deptId  WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null " +
                        //"   AND  " +
                        //"  ( " +
                        //" To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //" AND  " +
                        //"  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //"   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss') " +
                                    " ) " +
                                    " AND   " + QueryString + "   " +
                        //  " ( " +
                        //  " UPPER(TRIM(StatusReason)) IN   ('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')       OR    StatusReason like '%F.I.R Case - Missed Wire Pulled%'   ) " +
                                  "  )  " +
                                    " PIVOT  (     Count(ComplaintId)     FOR (MonthOfYear) in ('JAN' AS JAN,'FEB' as FEB,'MAR' AS MAR,'APR' as APRIL,'MAY' as MAY,'JUN' as JUN,'JUL' as JULY,'AUG' as AUG,'SEP' AS SEP,'OCT' as OCT,'NOV' as NOV, 'DEC' AS DEC)         ) ";



                    _commnadData.CommandText = Query;



                }
                else
                {

                    string Query = "   Select  *from ( " +
                                 "  SELECT To_Char(TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss'),'MON') MonthOfYear,D.DeptName,cn.MncName ,  ComplaintId " +
                                 "  FROM COMPLAINT C   " +
                                 "  inner join Centre Cn on trim(C.MNCCODE)=trim(Cn.MNCCODE) inner join Department D on Cn.DeptId=D.deptId  WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null " +
                                 "   AND  " +
                                 "  ( " +
                                 " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                 " AND  " +
                                 "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                 "   ) " +
                                 "  AND  " +
                                 "  ( " +
                                  "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss') " +
                                   " ) " +

                                 "  )   " +
                                   " PIVOT  (     Count(ComplaintId)     FOR (MonthOfYear) in ('JAN' AS JAN,'FEB' as FEB,'MAR' AS MAR,'APR' as APRIL,'MAY' as MAY,'JUN' as JUN,'JUL' as JULY,'AUG' as AUG,'SEP' AS SEP,'OCT' as OCT,'NOV' as NOV, 'DEC' AS DEC)         ) ";

                    _commnadData.CommandText = Query;
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }





        #endregion

        #region Slide 1....

        public DataTable TicketReceive(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {



                    string Query = "   Select   MonthofYear,Count(ComplaintId) from ( " +
                                  "  SELECT To_Char(TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss'),'MON-YY') MonthOfYear,  ComplaintId " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null " +
                                  "   AND  " +
                        // "  ( " +
                        // " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        // " AND  " +
                        // "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        // "   ) " +
                        // "  AND  " +
                                  "  ( " +
                                   "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss') " +
                                    " ) " +
                                    " AND   " + QueryString +
                                  "  )  " +
                                    "  Group by MonthofYear";



                    _commnadData.CommandText = Query;



                }
                else
                {

                    string Query = "   Select MonthofYear,Count(ComplaintId) from ( " +
                                  "  SELECT To_Char(TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss'),'MON-YY') MonthOfYear,  ComplaintId " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null " +
                                  "   AND  " +
                                  "  ( " +
                                  " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  " AND  " +
                                  "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  "   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss') " +
                                    " ) " +

                                  "  )  " +
                                    "  Group by MonthofYear ";
                    _commnadData.CommandText = Query;
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        public DataTable HTTicket(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {



                    string Query = "   Select   MonthofYear,Count(ComplaintId),round(AVG(TimeSlice),2) TATHT from ( " +
                                  "  SELECT To_Char(TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss'),'MON-YY') MonthOfYear,NVL(TimeSlice,'0') TimeSlice,  ComplaintId " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null AND ( upper(trim(StatusTo))='OPN' OR (trim(Status)='E0005' AND upper(trim(StatusTo))='OPN')) " +
                        //"   AND  " +
                        //"  ( " +
                        //" To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //" AND  " +
                        //"  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //"   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss') " +
                                    " ) " +
                                    " AND   " + QueryString +
                                  "  )  " +
                                    "  Group by MonthofYear";



                    _commnadData.CommandText = Query;



                }
                else
                {

                    string Query = "   Select MonthofYear,Count(ComplaintId),round(AVG(TimeSlice),2) TATHT from ( " +
                                  "  SELECT To_Char(TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss'),'MON-YY') MonthOfYear,NVL(TimeSlice,'0') TimeSlice,  ComplaintId " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null AND ( upper(trim(StatusTo))='OPN' OR (trim(Status)='E0005' AND upper(trim(StatusTo))='OPN')) " +
                                  "   AND  " +
                                  "  ( " +
                                  " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  " AND  " +
                                  "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  "   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss') " +
                                    " ) " +

                                  "  )  " +
                                    "  Group by MonthofYear ";
                    _commnadData.CommandText = Query;
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        public DataTable LTTicket(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {



                    string Query = "   Select   MonthofYear,Count(ComplaintId),round(AVG(TimeSlice),2) TATLT from ( " +
                                  "  SELECT To_Char(TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss'),'MON-YY') MonthOfYear,NVL(TimeSlice,'0') TimeSlice,  ComplaintId " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null AND ( upper(trim(StatusTo)) !='OPN' OR (trim(Status)='E0005' AND upper(trim(StatusTo)) !='OPN')) and StatusTo is not null " +
                        //"   AND  " +
                        //"  ( " +
                        //" To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //" AND  " +
                        //"  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //"   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss') " +
                                    " ) " +
                                    " AND   " + QueryString +
                                  "  )  " +
                                    "  Group by MonthofYear";



                    _commnadData.CommandText = Query;



                }
                else
                {

                    string Query = "   Select MonthofYear,Count(ComplaintId),round(AVG(TimeSlice),2) TATLT from ( " +
                                  "  SELECT To_Char(TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss'),'MON-YY') MonthOfYear,NVL(TimeSlice,'0') TimeSlice,  ComplaintId " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null AND ( upper(trim(StatusTo)) !='OPN' OR (trim(Status)='E0005' AND upper(trim(StatusTo)) !='OPN'))  and StatusTo is not null" +
                                  "   AND  " +
                                  "  ( " +
                                  " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  " AND  " +
                                  "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  "   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss') " +
                                    " ) " +

                                  "  )  " +
                                    "  Group by MonthofYear ";
                    _commnadData.CommandText = Query;
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable FaultLT(string DateFrom, string DateTo, string TimeFrom, string TimeTo,string IBCString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                 string Query="";
                if(IBCString !=null && IBCString !="")
                 Query = "   Select MonthofYear,Count(ComplaintId) from ( " +
                                  "  SELECT To_Char(TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss'),'MON-YY') MonthOfYear,  ComplaintId " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null AND upper(trim(StatusTo)) !='OPN'  and StatusTo is not null AND (trim(C.ParentTicketNo) ='0' OR C.ParentTicketNo is null)" +
                                  "   AND  " +
                                  "  ( " +
                                  " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  " AND  " +
                                  "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  "   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss') " +
                                    " ) " +IBCString+

                                  "  )  " +
                                    "  Group by MonthofYear ";
                else
                    Query = "   Select MonthofYear,Count(ComplaintId) from ( " +
                                 "  SELECT To_Char(TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss'),'MON-YY') MonthOfYear,  ComplaintId " +
                                 "  FROM COMPLAINT C   " +
                                 "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null AND upper(trim(StatusTo)) !='OPN'  and StatusTo is not null AND (trim(C.ParentTicketNo) ='0' OR C.ParentTicketNo is null)" +
                                 "   AND  " +
                                 "  ( " +
                                 " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                 " AND  " +
                                 "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                 "   ) " +
                                 "  AND  " +
                                 "  ( " +
                                  "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss') " +
                                   " ) " +

                                 "  )  " +
                                   "  Group by MonthofYear ";

                _commnadData.CommandText = Query;


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        public DataTable TATResolved(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {



                    string Query = "   Select   MonthofYear,ROUND(SUM(TimeSlice)/count(TimeSlice),2) TAT from ( " +
                                  "  SELECT To_Char(TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss'),'MON-YY') MonthOfYear,  TimeSlice " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null AND Status in('E0005','E0006') AND TimeSlice is not null " +
                        //"   AND  " +
                        //"  ( " +
                        //" To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //" AND  " +
                        //"  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //"   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss') " +
                                    " ) " +
                                    " AND   " + QueryString +
                                  "  )  " +
                                    "  Group by MonthofYear";



                    _commnadData.CommandText = Query;



                }
                else
                {

                    string Query = "   Select MonthofYear,ROUND(SUM(TimeSlice)/count(TimeSlice),2) TAT from ( " +
                                  "  SELECT To_Char(TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss'),'MON-YY') MonthOfYear,  TimeSlice " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null AND Status in('E0005','E0006') AND TimeSlice is not null " +
                                  "   AND  " +
                                  "  ( " +
                                  " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  " AND  " +
                                  "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  "   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss') " +
                                    " ) " +

                                  "  )  " +
                                    "  Group by MonthofYear ";
                    _commnadData.CommandText = Query;
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable LTWBFault(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {



                    string Query = "   Select   MonthofYear,count(ComplaintId)  from ( " +
                                  "  SELECT To_Char(TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss'),'MON-YY') MonthOfYear,  ComplaintId " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null  " +
                        //"   AND  " +
                        //"  ( " +
                        //" To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //" AND  " +
                        //"  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //"   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss') " +
                                    " ) AND " +
                                     " ( " +
                               " UPPER(TRIM(StatusReason)) IN   ('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')       OR    StatusReason like '%F.I.R Case - Missed Wire Pulled%'   ) " +

                                    " AND   " + QueryString +
                                  "  )  " +
                                    "  Group by MonthofYear";



                    _commnadData.CommandText = Query;



                }
                else
                {

                    string Query = "   Select MonthofYear,count(ComplaintId)  from ( " +
                                  "  SELECT To_Char(TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss'),'MON-YY') MonthOfYear,  ComplaintId " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null " +
                                  "   AND  " +
                                  "  ( " +
                                  " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  " AND  " +
                                  "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  "   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss') " +
                                    " ) AND " +
                                     " ( " +
                              " UPPER(TRIM(StatusReason)) IN   ('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')       OR    StatusReason like '%F.I.R Case - Missed Wire Pulled%'   ) " +

                                  "  )  " +
                                    "  Group by MonthofYear ";
                    _commnadData.CommandText = Query;
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable TicketReceiveCurrentYear(string Year, string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {



                    string Query = "  SELECT Count(ComplaintId) " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null " +
                        //"   AND  " +
                        //"  ( " +
                        //" To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //" AND  " +
                        //"  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //"   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'YYYY') =  '" + Year + "'  " +
                                    " ) " +
                                    " AND   " + QueryString;




                    _commnadData.CommandText = Query;



                }
                else
                {

                    string Query = "   SELECT Count(ComplaintId) " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null " +
                                  "   AND  " +
                                  "  ( " +
                                  " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  " AND  " +
                                  "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  "   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'YYYY') =  '" + Year + "'  " +
                                    " ) ";


                    _commnadData.CommandText = Query;
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable HTTicketByYear(string Year, string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {



                    string Query = "   SELECT Count(ComplaintId),round(AVG(TimeSlice),2) TAT " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null AND (upper(trim(StatusTo))='OPN' OR ( upper(trim(StatusTo))='OPN' AND trim(Status)='E0005')) " +
                        //"   AND  " +
                        //"  ( " +
                        //" To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //" AND  " +
                        //"  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //"   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'YYYY') =  '" + Year + "'  " +
                                    " ) " +
                                    " AND   " + QueryString;





                    _commnadData.CommandText = Query;



                }
                else
                {

                    string Query = "    SELECT count( ComplaintId),round(AVG(TimeSlice),2) TAT " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null AND  (upper(trim(StatusTo))='OPN' OR ( upper(trim(StatusTo))='OPN' AND trim(Status)='E0005')) " +
                                  "   AND  " +
                                  "  ( " +
                                  " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  " AND  " +
                                  "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  "   ) " +
                                  "  AND  " +
                                  "  ( " +
                                     "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'YYYY') =  '" + Year + "'  " +
                                    " ) ";
                    _commnadData.CommandText = Query;
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable LTTicketByYear(string Year, string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {



                    string Query = "    SELECT Count( ComplaintId),round(AVG(TimeSlice),2) TAT " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null AND (upper(trim(StatusTo)) !='OPN' OR ( upper(trim(StatusTo)) !='OPN' AND trim(Status)='E0005')) and StatusTo is not null " +
                        //"   AND  " +
                        //"  ( " +
                        //" To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //" AND  " +
                        //"  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //"   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'YYYY') =  '" + Year + "' " +
                                    " ) " +
                                    " AND   " + QueryString;




                    _commnadData.CommandText = Query;



                }
                else
                {

                    string Query = "   SELECT Count(ComplaintId),round(AVG(TimeSlice),2) TAT " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null AND  (upper(trim(StatusTo)) !='OPN' OR ( upper(trim(StatusTo)) !='OPN' AND trim(Status)='E0005'))  and StatusTo is not null" +
                                  "   AND  " +
                                  "  ( " +
                                  " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  " AND  " +
                                  "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  "   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'YYYY') =  '" + Year + "' " +
                                    " ) ";


                    _commnadData.CommandText = Query;
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        public DataTable FaultLTByYear(string Year, string DateFrom, string DateTo, string TimeFrom, string TimeTo,string IBCCode)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                 string Query="";
                 if (IBCCode != "" && IBCCode != null)
                 {
                     Query = "   SELECT count(ComplaintId) " +
                                    "  FROM COMPLAINT C   " +
                                    "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null AND upper(trim(StatusTo)) !='OPN'  and StatusTo is not null AND (trim(C.ParentTicketNo) ='0' OR C.ParentTicketNo is null)" +
                                    "   AND  " +
                                    "  ( " +
                                    " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                    " AND  " +
                                    "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                    "   ) " +
                                    "  AND  " +
                                    "  ( " +
                                     "   To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'YYYY') =  '" + Year + "'  " +
                                      " ) " +IBCCode;
                 }
                 else
                 {
                     Query = "   SELECT count(ComplaintId) " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null AND upper(trim(StatusTo)) !='OPN'  and StatusTo is not null AND (trim(C.ParentTicketNo) ='0' OR C.ParentTicketNo is null)" +
                                  "   AND  " +
                                  "  ( " +
                                  " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  " AND  " +
                                  "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  "   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "   To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'YYYY') =  '" + Year + "'  " +
                                    " ) ";
                 }


                _commnadData.CommandText = Query;


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable TATResolvedByYear(string Year, string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {



                    string Query = " select  ROUND(SUM(TimeSlice)/count(TimeSlice),2) " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null AND Status in('E0005','E0006') AND TimeSlice is not null " +
                        //"   AND  " +
                        //"  ( " +
                        //" To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //" AND  " +
                        //"  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //"   ) " +
                                  "  AND  " +
                                  "  ( " +
                                     "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'YYYY') =  '" + Year + "' " +
                                    " ) " +
                                    " AND   " + QueryString;




                    _commnadData.CommandText = Query;



                }
                else
                {

                    string Query = "  select ROUND(SUM(TimeSlice)/count(TimeSlice),2)  " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null AND Status in('E0005','E0006') AND TimeSlice is not null " +
                                  "   AND  " +
                                  "  ( " +
                                  " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  " AND  " +
                                  "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  "   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'YYYY') =  '" + Year + "' " +
                                    " ) ";
                    _commnadData.CommandText = Query;
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable LTWBFaultByYear(string Year, string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {



                    string Query = "   Select   count(ComplaintId)   " +
                                  "  FROM COMPLAINT C   " +
                        "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null   " +
                        //"   AND  " +
                        //"  ( " +
                        //" To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //" AND  " +
                        //"  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //"   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'YYYY') =  '" + Year + "' " +
                                    " ) AND " +
                                     " ( " +
                               " UPPER(TRIM(StatusReason)) IN   ('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')       OR    StatusReason like '%F.I.R Case - Missed Wire Pulled%'   ) " +

                                    " AND   " + QueryString;




                    _commnadData.CommandText = Query;



                }
                else
                {

                    string Query = " Select count(ComplaintId)  " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null " +
                                  "   AND  " +
                                  "  ( " +
                                  " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + DateFrom + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  " AND  " +
                                  "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + DateTo + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  "   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'YYYY') =  '" + Year + "' " +
                                    " ) AND " +
                                     " ( " +
                              " UPPER(TRIM(StatusReason)) IN   ('SERVICE WIRE BROKEN','MAIN WIRE BROKEN','WIRE BROKEN','WIRE MISSED')       OR    StatusReason like '%F.I.R Case - Missed Wire Pulled%'   ) ";


                    _commnadData.CommandText = Query;
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        #endregion

        #region Re Open Summary Ticket Wise...

       

        // ReOpen Ticket in Month
        public DataTable getReOpenTicketOfMonth(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString, string MonthQuery)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;

                if (QueryString != "" && QueryString != null)
                {
                    _commnadData.CommandText = "select *  from " +
                          "( SELECT Upper(trim(rpn_statusreason)) ReOpenStatus ,Upper(rpn_statusreason) ReOpenStatus1  ,trim(To_Char(TO_DATE(RPN_DATETIME,'yyyyMMddHH24MIss'),'MON-YY')) MonthYear " +

                          " FROM RPNSUMMARY " +
                             " Where (CompleteStatusReason is not null AND rpn_statusreason is not null)  and   " + QueryString + " AND " +
                        //"     " +
                        //      "  ( " +
                        //      " To_Char(To_Date(RPN_DateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //      " AND  " +
                        //      "  To_Char(To_Date(RPN_DateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //      "   ) " +
                        //      "  AND  " +
                                  "  ( " +
                                   "  To_Date(RPN_DateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                    " ) " +

                            "  )pivot (count(ReOpenStatus1) for  monthyear in(" + MonthQuery + ") ) order by ReOpenStatus ASC "; ;
                }

                else
                {
                    _commnadData.CommandText = "select *  from " +
                          "( SELECT Upper(trim(rpn_statusreason)) ReOpenStatus ,Upper(rpn_statusreason) ReOpenStatus1  ,trim(To_Char(TO_DATE(RPN_DATETIME,'yyyyMMddHH24MIss'),'MON-YY')) MonthYear " +

                          " FROM RPNSUMMARY " +
                             " Where(CompleteStatusReason is not null AND rpn_statusreason is not null)   AND " +
                            "     " +
                                  "  ( " +
                                  " To_Char(To_Date(RPN_DateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  " AND  " +
                                  "  To_Char(To_Date(RPN_DateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  "   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Date(RPN_DateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                    " ) " +

                            "  )pivot (count(ReOpenStatus1) for  monthyear in(" + MonthQuery + ") ) order by ReOpenStatus ASC "; ;
                }




                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        //End of ReOpen Ticket in Month
        public DataTable getReOpenTicketByCompleteStatusReason(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString, string CompleteStatusReason)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;

                if (QueryString != "")
                {
                    _commnadData.CommandText = "select *  from " +
                                                 "( SELECT  upper(trim(rpn_statusreason)) ReOpenStatus ,Upper(rpn_statusreason) ReOpenStatus1  ,upper(Trim(Completestatusreason)) Completestatusreason " +

                                                 " FROM RPNSUMMARY " +
                                                    " Where (CompleteStatusReason is not null AND rpn_statusreason is not null)  and   " + QueryString +
                        //"     " +
                        //"  ( " +
                        //" To_Char(To_Date(RPN_DateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //" AND  " +
                        //"  To_Char(To_Date(RPN_DateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //"   ) " +
                                                         "  AND  " +
                                                         "  ( " +
                                                          "  To_Date(RPN_DateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                                           " ) " +

                                                   "  )pivot (count(ReOpenStatus1) for  Completestatusreason in(" + CompleteStatusReason + ") ) order by ReOpenStatus ASC ";

                }
                else
                {
                    _commnadData.CommandText = "select *  from " +
                               "( SELECT  upper(trim(rpn_statusreason)) ReOpenStatus ,Upper(rpn_statusreason) ReOpenStatus1  ,upper(Trim(Completestatusreason)) Completestatusreason " +

                               " FROM RPNSUMMARY " +
                                  " Where (CompleteStatusReason is not null AND rpn_statusreason is not null)  and   " +
                                 "     " +
                                       "  ( " +
                                       " To_Char(To_Date(RPN_DateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                       " AND  " +
                                       "  To_Char(To_Date(RPN_DateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                       "   ) " +
                                       "  AND  " +
                                       "  ( " +
                                        "  To_Date(RPN_DateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                         " ) " +

                                 "  )pivot (count(ReOpenStatus1) for  Completestatusreason in(" + CompleteStatusReason + ") ) order by ReOpenStatus ASC ";




                }

                // _commnadData.CommandText = Query;


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetReOpenCompleteStatus(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString, string MonthQuery)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {


                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = " select Upper(trim(rpn_statusreason)) Rpn_StatusReason,trim(To_Char(TO_DATE(RPN_DATETIME,'yyyyMMddHH24MIss'),'MON-YY')) MonthYear, Upper(trim(CompleteStatusReason)) CompleteStatusReason FROM RPNSUMMARY " +
                              " Where (CompleteStatusReason is not null AND rpn_statusreason is not null) and   " + QueryString +

                                   //"  ( " +
                        //" To_Char(To_Date(RPN_DateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //" AND  " +
                        //"  To_Char(To_Date(RPN_DateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //"   ) " +
                                   "  AND  " +
                                   "  ( " +
                                    "  To_Date(RPN_DateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                     " ) ";

                }
                else
                {
                    _commnadData.CommandText = " select Upper(trim(rpn_statusreason)) Rpn_StatusReason,trim(To_Char(TO_DATE(RPN_DATETIME,'yyyyMMddHH24MIss'),'MON-YY')) MonthYear, Upper(trim(CompleteStatusReason)) CompleteStatusReason FROM RPNSUMMARY " +
                                " Where(CompleteStatusReason is not null AND rpn_statusreason is not null) and   " +
                               "     " +
                                     "  ( " +
                                     " To_Char(To_Date(RPN_DateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                     " AND  " +
                                     "  To_Char(To_Date(RPN_DateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                     "   ) " +
                                     "  AND  " +
                                     "  ( " +
                                      "  To_Date(RPN_DateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                       " ) ";




                }




                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        //Total Month Complaint
        public DataTable getReOpenTotalComplaint(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString, string MonthQuery)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = " SELECT * FROM (select ComplaintId, trim(To_Char(TO_DATE(ComplaintDateTime,'yyyyMMddHH24MIss'),'MON-YY')) MonthYear from complaint " +
                                 " where ComplaintDateTime !='0' and Complaintdatetime is not null AND " +
                                 " (To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss')) " +
                                  "   and   " + QueryString +
                        //"   (  " +
                        //"   To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +

                                  //"   and " +

                                  // "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        // "  ) "
                                    " ) " +


                            "  pivot (count(cOMPLAINTID) for  monthyear in(" + MonthQuery + ") )  ";
                }
                else
                {
                    _commnadData.CommandText = " SELECT * FROM (select ComplaintId, trim(To_Char(TO_DATE(ComplaintDateTime,'yyyyMMddHH24MIss'),'MON-YY')) MonthYear from complaint " +
                               " where ComplaintDateTime !='0' and Complaintdatetime is not null AND " +
                               " (To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss')) " +
                                "   and   " +
                                "   (  " +
                                "   To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +

                                "   and " +

                                 "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                 "  )) " +


                          "  pivot (count(cOMPLAINTID) for  monthyear in(" + MonthQuery + ") )  ";
                }





                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        //End Of Total Month Complaint
        //Second Grid Binding Method
        public DataTable GetRPNComplaintByReOpenDate(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString, string Month, string ReOpenStatusReason)
        {
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = " SELECT * FROM RPNSUMMARY " +
                                // " where (CompleteStatusReason is not null AND rpn_statusreason is not null) and   To_Char(To_Date(Rpn_DateTime,'yyyyMMddHH24MISS'),'MON-YY')='" + Month + "'  " + ReOpenStatusReason +
                                 " where    To_Char(To_Date(Rpn_DateTime,'yyyyMMddHH24MISS'),'MON-YY')='" + Month + "'  " + ReOpenStatusReason +

                                 " and  (To_Date(RPN_DATETIME,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss')) " +
                                  "   and   " + QueryString + "Order By CRMNO";

                }
                else
                {
                    _commnadData.CommandText = " SELECT * FROM RPNSUMMARY " +
                               //  " where (CompleteStatusReason is not null AND rpn_statusreason is not null) and  To_Char(To_Date(Rpn_DateTime,'yyyyMMddHH24MISS'),'MON-YY')='" + Month + "'  " + ReOpenStatusReason +
                                 " where   To_Char(To_Date(Rpn_DateTime,'yyyyMMddHH24MISS'),'MON-YY')='" + Month + "'  " + ReOpenStatusReason +

                                 " and (To_Date(RPN_DATETIME,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss')) " +

                                  " AND (To_Date(RPN_DATETIME,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss')) " +
                                "   and   " +
                                "   (  " +
                                "   To_Char(To_Date(RPN_DATETIME,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +

                                "   and " +

                                 "  To_Char(To_Date(RPN_DATETIME,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                 "  )  Order By CRMNO";


                }






                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetRPNComplaintByCompleteStatusReason(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString, string CompleteStatusReason, string ReOpenStatusReason)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = " SELECT * FROM RPNSUMMARY " +
                                 " where (CompleteStatusReason is not null AND rpn_statusreason is not null) and   upper(trim(CompleteStatusReason)) like '%" + CompleteStatusReason + "%' " + ReOpenStatusReason +
                                 " and (To_Date(RPN_DATETIME,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss')) " +
                                  "   and   " + QueryString + "Order By CRMNO";

                }
                else
                {
                    _commnadData.CommandText = " SELECT * FROM RPNSUMMARY " +
                              " where(CompleteStatusReason is not null AND rpn_statusreason is not null) and  upper(trim(CompleteStatusReason)) like '%" + CompleteStatusReason + "%' " + ReOpenStatusReason +
                                 " and  (To_Date(RPN_DATETIME,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss')) " +
                                "   and   " +
                                "   (  " +
                                "   To_Char(To_Date(RPN_DATETIME,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +

                                "   and " +

                                 "  To_Char(To_Date(RPN_DATETIME,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                 "  ) Order By CRMNO";


                }





                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }
        //End of Second  Grid Binding  Data


        //Totat TAT in Each Month
        public DataTable GetTATOfReOpenTicket(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = " SELECT To_Char(To_Date(RPN_DATETIME,'yyyyMMddHH24MIss'),'MON-YY') MONTH ,SUBSTR(complete_RECOPENTAT,0,2)||'.'|| SUBSTR(complete_RECOPENTAT,4,2) Complete_REOPENTAT  FROM RPNSUMMARY " +
                                 " where (CompleteStatusReason is not null AND rpn_statusreason is not null)  " +
                                 " and (To_Date(RPN_DATETIME,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss')) " +
                                  "   and   " + QueryString;

                }
                else
                {
                    _commnadData.CommandText = " SELECT To_Char(To_Date(RPN_DATETIME,'yyyyMMddHH24MIss'),'MON-YY') MONTH, SUBSTR(complete_RECOPENTAT,0,2)||'.'|| SUBSTR(complete_RECOPENTAT,4,2) Complete_REOPENTAT  FROM RPNSUMMARY " +
                              " where (CompleteStatusReason is not null AND rpn_statusreason is not null)  " +
                                 " and  (To_Date(RPN_DATETIME,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss')) " +
                                "   and   " +
                                "   (  " +
                                "   To_Char(To_Date(RPN_DATETIME,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +

                                "   and " +

                                 "  To_Char(To_Date(RPN_DATETIME,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                 "  ) ";


                }





                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetZeroLevelInfoByCRMNO(string crmString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;

                _commnadData.CommandText = " SELECT CRMNO,LEVELOFREOPEN,  LineManName,ShiftIncharge,CompleteStatusReason  from Rpnsummary where crmno in (" + crmString + ") and LevelOfReOpen=0 order by CRMNO   ";
                                 

               
                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }
        //

        #endregion

        #region ZOne Wise


        public DataTable GetComplaintByZoneAndStatusTo(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {

                    string Query = "   select *  from (select TRIM(UPPER(StatusTo)) StatusTo,Zone,count(StatusTo) Total from Complaint where status in('E0005','E0006') and zone is not null and (UPPER(StatusTo) is not null OR trim(UPPER(StatusTo)) !='') " +

                                  " AND   trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null and trim(Zone) is not null  and trim(statusto) is not null " +
                                  "   AND  " +
                        //"  ( " +
                        //"  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //" AND  " +
                        //"  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //"   ) " +
                        //"  AND  " +
                                  "  ( " +
                                   "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                    " ) " +
                                    " AND   " + QueryString +

                                     "  group by statusTo,Zone)" +

                                    "  pivot ( SUM(Total) for(StatusTo) in ('BREAKER TRIP','JUMPER REPAIRED','WIRE','OTHER','ABC','SSM','UGM','OPN','RPR','MM','TESTING','HT') ) ";



                    _commnadData.CommandText = Query;



                }
                else
                {

                    string Query = "   select *  from (select TRIM(UPPER(StatusTo)) StatusTo,Zone,count(StatusTo) Total from Complaint where status in('E0005','E0006') and zone is not null and (UPPER(StatusTo) is not null OR trim(UPPER(StatusTo)) !='') " +

                                  "  AND  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null and trim(Zone) is not null  and trim(statusto) is not null " +
                                  "   AND  " +
                                  "  ( " +
                                  " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  " AND  " +
                                  "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                  "   ) " +
                                  "  AND  " +
                                  "  ( " +
                                   "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                    " ) " +
                                    "  group by statusTo,Zone)" +

                                    "  pivot ( SUM(Total) for(StatusTo) in ('BREAKER TRIP','JUMPER REPAIRED','WIRE','OTHER','ABC','SSM','UGM','OPN','RPR','MM','TESTING','HT') )";

                    _commnadData.CommandText = Query;
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        public DataTable GetTop10DTSByStatusReasonAndZone(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString, string Zone = "", string StatusTo = "")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {


                    _commnadData.CommandText = "select  To_Char(to_Date(cOMPLAINTDATETime,'yyyyMMddHH24MIss'),'dd-MM-yy') ReceiveDate, DTSID,pmtname,count(*) Total, " +
                                                " MIN(To_Char(to_Date(cOMPLAINTDATETime,'yyyyMMddHH24MIss'),'HH24.MI')) AS MinimumReceiveTime ,max(To_Char(to_Date(cOMPLAINTDATETime,'yyyyMMddHH24MIss'),'HH24.MI')) AS MaximumReceiveTime from COmplaint " +
                                                 " where status in('E0005','E0006') and trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null and trim(Zone) is not null  and  " + Zone + " and " + StatusTo + " AND " + QueryString +
                                                  " AND  ( " +
                                                   "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                                    " ) " +
                                               //  "  and dtsid is not null    " +
                                                " group by To_Char(to_Date(cOMPLAINTDATETime,'yyyyMMddHH24MIss'),'dd-MM-yy'), DTSID,pmtname " +
                                                    " order by To_Char(to_Date(cOMPLAINTDATETime,'yyyyMMddHH24MIss'),'dd-MM-yy') DESC";



                }
                else
                {

                    _commnadData.CommandText = "select  To_Char(to_Date(cOMPLAINTDATETime,'yyyyMMddHH24MIss'),'dd-MM-yy') ReceiveDate, DTSID,pmtname,count(*) Total, " +
                                                " MIN(To_Char(to_Date(cOMPLAINTDATETime,'yyyyMMddHH24MIss'),'HH24.MI')) AS MinimumReceiveTime ,max(To_Char(to_Date(cOMPLAINTDATETime,'yyyyMMddHH24MIss'),'HH24.MI')) AS MaximumReceiveTime from COmplaint " +
                                                 " where status in('E0005','E0006') and trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null and trim(Zone) is not null  " +
                                                  " AND   ( " +
                                                   "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                                    " ) " +
                                                       " AND   ( " +
                                                          " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=     To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                                          " AND  " +
                                                          "  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=     To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                                          "   ) and  " + Zone + " and " + StatusTo +


                                                " group by To_Char(to_Date(cOMPLAINTDATETime,'yyyyMMddHH24MIss'),'dd-MM-yy'), DTSID,pmtname " +
                                                    " order by To_Char(to_Date(cOMPLAINTDATETime,'yyyyMMddHH24MIss'),'dd-MM-yy') DESC";

                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        #endregion

        #region TAT Summary........Report...


        public DataTable GetCurrentYearTatSummary(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string CurrentYear, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = "SELECT SR.RPTTATSUMMARYHEADER,ROUND((SUM(SR.ASSIGNTAT)/COUNT(sr.RPTTATSUMMARYHEADER)),2) AVGTATS FROM COMPLAINT C " +
                                                " inner JOIN STATUSREASON SR ON UPPER(TRIM(SR.REASON))=UPPER(TRIM(C.STATUSREASON)) " +
                                                " WHERE C.COMPLAINTDATETIME IS NOT NULL AND TRIM(C.COMPLAINTDATETIME) !='0' AND C.STATUSREASON IS NOT NULL AND SR.RPTTATSUMMARYHEADER IS NOT NULL AND " +
                                                " To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                                "  AND  TO_CHAR(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'yyyy')='" + CurrentYear + "'  " +
                        //" AND ( " +
                        //   " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >= To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //   " AND " +
                        //   " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <= To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //   " ) "+
                                                    " AND " + QueryString + " " +
                                                    " GROUP BY RPTTATSUMMARYHEADER ";
                }
                else
                {
                    _commnadData.CommandText = "SELECT SR.RPTTATSUMMARYHEADER,ROUND((SUM(SR.ASSIGNTAT)/COUNT(sr.RPTTATSUMMARYHEADER)),2) AVGTATS FROM COMPLAINT C " +
                                               " inner JOIN STATUSREASON SR ON UPPER(TRIM(SR.REASON))=UPPER(TRIM(C.STATUSREASON)) " +
                                               " WHERE C.COMPLAINTDATETIME IS NOT NULL AND TRIM(C.COMPLAINTDATETIME) !='0' AND C.STATUSREASON IS NOT NULL AND SR.RPTTATSUMMARYHEADER IS NOT NULL AND " +
                                               " To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                               "  AND  TO_CHAR(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'yyyy')='" + CurrentYear + "'  " +
                                                " AND ( " +
                                                   " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >= To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                                   " AND " +
                                                   " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <= To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                                   " ) " +
                                                   " GROUP BY RPTTATSUMMARYHEADER ";
                }



                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable GetLastYearTatSummary(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string LastYear, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = "SELECT SR.RPTTATSUMMARYHEADER,ROUND((SUM(SR.ASSIGNTAT)/COUNT(sr.RPTTATSUMMARYHEADER)),2) AVGTATS FROM COMPLAINT C " +
                                        " left JOIN STATUSREASON SR ON UPPER(TRIM(SR.REASON))=UPPER(TRIM(C.STATUSREASON)) " +
                                        " WHERE C.COMPLAINTDATETIME IS NOT NULL AND TRIM(C.COMPLAINTDATETIME) !='0' AND C.STATUSREASON IS NOT NULL AND SR.RPTTATSUMMARYHEADER IS NOT NULL AND " +
                                        " To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                        "  AND  TO_CHAR(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'yyyy')='" + LastYear + "'  " +
                        //" AND ( " +
                        //       " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >= To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //       " AND " +
                        //       " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <= To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                        //       " ) "+
                                                " AND  " + QueryString + " " +
                                            " GROUP BY RPTTATSUMMARYHEADER ";
                }
                else
                {

                    _commnadData.CommandText = "SELECT SR.RPTTATSUMMARYHEADER,ROUND((SUM(SR.ASSIGNTAT)/COUNT(sr.RPTTATSUMMARYHEADER)),2) AVGTATS FROM COMPLAINT C " +
                                            " left JOIN STATUSREASON SR ON UPPER(TRIM(SR.REASON))=UPPER(TRIM(C.STATUSREASON)) " +
                                            " WHERE C.COMPLAINTDATETIME IS NOT NULL AND TRIM(C.COMPLAINTDATETIME) !='0' AND C.STATUSREASON IS NOT NULL AND SR.RPTTATSUMMARYHEADER IS NOT NULL AND " +
                                            " To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                            "  AND  TO_CHAR(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'yyyy')='" + LastYear + "'  " +
                                             " AND ( " +
                                                    " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >= To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                                    " AND " +
                                                    " To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <= To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                                    " ) " +
                                                " GROUP BY RPTTATSUMMARYHEADER ";
                }




                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        public DataTable getTatSummaryComplaint(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {



                    string Query = "  SELECT SR.RPTTATSUMMARYHEADER, To_Char(TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss'),'MON-YY')   MonthOfYear, STATUSREASON,ROUND(TRIM(TIMESLICE),2) TIMESLICE  FROM COMPLAINT C  " +
                                    "  INNER JOIN STATUSREASON SR ON UPPER(TRIM(C.STATUSREASON))=UPPER(TRIM(SR.REASON)) " +
                                    " WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null   AND TRIM(C.STATUS) ='E0005' " +
                        //  " AND    (  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=    To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI')  AND   		 To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=  		   To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')    )  " +
                                    "AND    (   To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss')  ) AND  " + QueryString;



                    _commnadData.CommandText = Query;



                }
                else
                {

                    string Query = "  SELECT SR.RPTTATSUMMARYHEADER, To_Char(TO_DATE(ComplaintDateTime,    'yyyyMMddHH24MIss'),'MON-YY')   MonthOfYear, STATUSREASON,ROUND(TRIM(TIMESLICE),2) TIMESLICE  FROM COMPLAINT C  " +
                                "  INNER JOIN STATUSREASON SR ON UPPER(TRIM(C.STATUSREASON))=UPPER(TRIM(SR.REASON)) " +
                                " WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null   AND TRIM(C.STATUS) ='E0005' " +
                                " AND    (  To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') >=    To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI')  AND   		 To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24.MI') <=  		   To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI')    )  " +
                                "AND    (   To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss')  )  ";


                    _commnadData.CommandText = Query;
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }




        #endregion

        #region Puls Report...

        public DataTable getPulsPendingTicket(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = " select cn.MNCNAME ,Count(*) Pending from Complaint C inner join centre cn on C.mnccode=cn.mnccode where " +
                        " (( " +
                        " (           (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + TimeFrom + "',    'yyyyMMddHH24MIss')))        AND  " +
                        " (            (STATUS    NOT    IN    ('E0005',    'E0006'))        OR  " +
                        " ( (STATUS    IN    ('E0005',    'E0006')) AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + TimeTo + "',    'yyyyMMddHH24MIss'))) )  " +
                        " ) " +
                        " OR " +
                        " ( " +
                        " (STATUS    IN    ('E0005',    'E0006'))  " +
                        " AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + TimeTo + "',    'yyyyMMddHH24MIss')) " +
                         " AND  (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')  BETWEEN    TO_DATE('" + TimeFrom + "',    'yyyyMMddHH24MIss')  AND     TO_DATE('" + TimeTo + "',    'yyyyMMddHH24MIss') )     " +
                        " ) OR " +
                        " ( TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')     " +
                        "  BETWEEN    TO_DATE('" + TimeFrom + "',    'yyyyMMddHH24MIss')  AND     TO_DATE('" + TimeTo + "',    'yyyyMMddHH24MIss') " +
                        " )) AND " + QueryString + "group by cn.MNCNAME";


                }
                else
                {


                    _commnadData.CommandText = " select cn.MNCNAME ,Count(*) Pending from Complaint C inner join centre cn on C.mnccode=cn.mnccode where " +
                         " ( " +
                         " (           (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')    <    TO_DATE('" + TimeFrom + "',    'yyyyMMddHH24MIss')))        AND  " +
                         " (            (STATUS    NOT    IN    ('E0005',    'E0006'))        OR  " +
                         " ( (STATUS    IN    ('E0005',    'E0006')) AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + TimeTo + "',    'yyyyMMddHH24MIss'))) )  " +
                         " ) " +
                         " OR " +
                         " ( " +
                         " (STATUS    IN    ('E0005',    'E0006'))  " +
                         " AND  (TO_DATE(COMPLAINTATTEMPTDATETIME,    'yyyyMMddHH24MIss')    >        TO_DATE('" + TimeTo + "',    'yyyyMMddHH24MIss')) " +
                          " AND  (TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')  BETWEEN    TO_DATE('" + TimeFrom + "',    'yyyyMMddHH24MIss')  AND     TO_DATE('" + TimeTo + "',    'yyyyMMddHH24MIss') )     " +
                         " ) OR " +
                         " ( TO_DATE(COMPLAINTDATETIME,    'yyyyMMddHH24MIss')     " +
                         "  BETWEEN    TO_DATE('" + TimeFrom + "',    'yyyyMMddHH24MIss')  AND     TO_DATE('" + TimeTo + "',    'yyyyMMddHH24MIss') " +
                         " ) group by cn.MNCNAME";

                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        public DataTable getPendingHTShutDown(string DateFrom, string DateTo,string IBCCODE)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;

                if (IBCCODE != null && IBCCODE != "")
                {
                    _commnadData.CommandText = " SELECT DISTINCT OUTAGERECORD.DateTime AS \"Date Time\",DTSDETAILS.FEEDERNAME ||'/'|| DTSDETAILS.TRAFONAME as \"PMT / FEEDER NAME\",DTSDETAILS.DTS, " +
                                                " OUTAGERECORD.FwdTime AS \"Fwd Time\",OUTAGERECORD.AttendTime as\"Attend Time\",OUTAGERECORD.Outage AS \"Nature Of Outage\",'This Is Dummy data' As \"ShutDown By / Fault By\",OUTAGERECORD.DETAIL FROM OUTAGERECORD  " +
                                                "inner join Centre Cn on Cn.MNCName=OUTAGERECORD.MncName and Cn.MNCCODE in (" + IBCCODE + ") " +
                                                 " INNER  JOIN DTSDETAILS ON DTSDETAILS.FEEDERID = OUTAGERECORD.FEEDERID " +
                                                 " where to_date(datetime) between to_date('" + DateFrom + "','mm/dd/yyyy') and to_date('" + DateTo + "','mm/dd/yyyy')  order by datetime asc";
                }
                else
                {
                    _commnadData.CommandText = " SELECT DISTINCT OUTAGERECORD.DateTime AS \"Date Time\",DTSDETAILS.FEEDERNAME ||'/'|| DTSDETAILS.TRAFONAME as \"PMT / FEEDER NAME\",DTSDETAILS.DTS, " +
                                                 " OUTAGERECORD.FwdTime AS \"Fwd Time\",OUTAGERECORD.AttendTime as\"Attend Time\",OUTAGERECORD.Outage AS \"Nature Of Outage\",'This Is Dummy data' As \"ShutDown By / Fault By\",OUTAGERECORD.DETAIL FROM OUTAGERECORD  " +
                                               
                                                  " INNER  JOIN DTSDETAILS ON DTSDETAILS.FEEDERID = OUTAGERECORD.FEEDERID " +
                                                  " where to_date(datetime) between to_date('" + DateFrom + "','mm/dd/yyyy') and to_date('" + DateTo + "','mm/dd/yyyy')  order by datetime asc";
                }
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        #endregion

        #region Team KPI...
        public DataTable GetLineManAndNoOfDays(string DateFrom, string DateTo, string IBCCODE)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (IBCCODE != "" && IBCCODE != null)
                    _commnadData.CommandText = "Select GangName,count(days) As Days from(select Distinct DT.GangName ,  To_Char(ShiftDate,'yyyyMMdd') AS Days from DutyRooster D   " +
                                               " inner join DutyRoosterTeam DT on D.DUtyroosterid=DT.DutyRoosterid  inner join centre C on D.Centre=C.MNCName and C.Mnccode in (" + IBCCODE + ")        " +
                                               " where shiftsignoutTime is not null and ShiftSignInTime is not null           " +
                                          "  AND To_Date(To_Char(ShiftDate,'yyyyMMdd'),'yyyyMMdd') " +
                                            " between  To_Date('" + DateFrom + "','yyyyMMdd') AND      To_Date('" + DateTo + "','yyyyMMdd')" +
                                            "  )    group By GangName  Order By GangName ASC   ";
                else
                    _commnadData.CommandText = "Select GangName,count(days) As Days from(select Distinct DT.GangName ,  To_Char(ShiftDate,'yyyyMMdd') AS Days from DutyRooster D   " +
                                          " inner join DutyRoosterTeam DT on D.DUtyroosterid=DT.DutyRoosterid         " +
                                          " where shiftsignoutTime is not null and ShiftSignInTime is not null           " +
                                     "  AND To_Date(To_Char(ShiftDate,'yyyyMMdd'),'yyyyMMdd') " +
                                       " between  To_Date('" + DateFrom + "','yyyyMMdd') AND      To_Date('" + DateTo + "','yyyyMMdd')" +
                                       "  )    group By GangName  Order By GangName ASC   ";


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }




        public DataTable GetShiftInchargeAndNoOfDays(string DateFrom, string DateTo, string IBCCODE)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (IBCCODE != "" && IBCCODE != null)
                    _commnadData.CommandText = "Select GangName,count(days) As Days from(select Distinct U.UserFullName GangName ,  To_Char(ShiftDate,'yyyyMMdd') AS Days from DutyRooster D   " +
                                               " inner join Users U  on D.ShiftInchargeid=U.UserID  inner join centre C on D.Centre=C.MNCName and C.Mnccode in (" + IBCCODE + ")        " +
                                               " where shiftsignoutTime is not null and ShiftSignInTime is not null           " +
                                          "  AND To_Date(To_Char(ShiftDate,'yyyyMMdd'),'yyyyMMdd') " +
                                            " between  To_Date('" + DateFrom + "','yyyyMMdd') AND      To_Date('" + DateTo + "','yyyyMMdd')" +
                                            "  )    group By GangName  Order By GangName ASC   ";
                else
                    _commnadData.CommandText = "Select GangName,count(days) As Days from(select Distinct U.UserFullName GangName ,  To_Char(ShiftDate,'yyyyMMdd') AS Days from DutyRooster D   " +
                                          " inner join Users U  on D.ShiftInchargeid=U.UserID         " +
                                          " where shiftsignoutTime is not null and ShiftSignInTime is not null           " +
                                     "  AND To_Date(To_Char(ShiftDate,'yyyyMMdd'),'yyyyMMdd') " +
                                       " between  To_Date('" + DateFrom + "','yyyyMMdd') AND      To_Date('" + DateTo + "','yyyyMMdd')" +
                                       "  )    group By GangName  Order By GangName ASC   ";


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }


        public DataTable GetTeamKPIComplaintBetweenDateRange(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {
                    _commnadData.CommandText = " select   distinct   MTL,NVL(LineMan_Name,' ') AS\"Line Man Name\",S.StatusName AS Status,crmserviceticket_no,Time_Slice,NVL(Status_To,' ') Status_To,NVL(Status_Reason,' ') Status_Reason,ReOpenCount,NVL(SI,' ') SI    from COmplaints_log inner join status S on Status=s.StatusCode " +
                                              "Where  Complaint_DateTime is not null and  trim(Complaint_DateTime) !='0' and  " +
                                              " To_Date(Complaint_DateTime,'yyyyMMddHH24MIss') between To_Date('" + TimeFrom + "','yyyyMMddHH24MIss') and To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                      
                                                " ANd (Status in ('E0005') or (status IN('RJCT') and upper(trim(status_reason))='SHIFT SIGN OFF') or (status IN('E0006') and upper(trim(Status_To))='OPN')  OR (status IN('E0003') and complaintAttempt_DateTime !='' and complaintAttempt_DateTime is not null AND To_Date(complaintAttempt_DateTime,'yyyyMMddHH24MIss') between To_Date('" + DateFrom + "','yyyyMMdd') and To_Date('" + DateTo + "','yyyyMMdd') ) ) and MTL is not NULL and LineMan_Name is not null " +
                                              "  AND " + QueryString + "  order by crmserviceticket_no ASC";

                }
                else
                {

                    _commnadData.CommandText = " select   distinct   MTL,NVL(LineMan_Name,' ') AS\"Line Man Name\",S.StatusName AS Status,crmserviceticket_no, Time_Slice,NVL(Status_To,' ') Status_To,NVL(Status_Reason,' ') Status_Reason,ReOpenCount,NVL(SI,' ') SI     from COmplaints_log inner join status S on Status=s.StatusCode " +
                                               "Where  Complaint_DateTime is not null and  trim(Complaint_DateTime) !='0' and  " +
                                               " To_Date(Complaint_DateTime,'yyyyMMddHH24MIss') between To_Date('" + TimeFrom + "','yyyyMMddHH24MIss') and To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                                " AND ( " +
                                                 " To_Char(To_Date(Complaint_DateTime,'yyyyMMddHH24MIss'),'HH24.MI') >= To_Char(To_Date('" + TimeFrom + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                                 " AND " +
                                                 " To_Char(To_Date(Complaint_DateTime,'yyyyMMddHH24MIss'),'HH24.MI') <= To_Char(To_Date('" + TimeTo + "','yyyyMMddHH24MIss'),'HH24.MI') " +
                                                 " ) " +
                                                 " ANd (Status in ('E0005') or (status IN('RJCT') and upper(trim(status_reason))='SHIFT SIGN OFF') or (status IN('E0006') and upper(trim(Status_To))='OPN')  OR (status IN('E0003') and complaintAttempt_DateTime !='' and complaintAttempt_DateTime is not null AND To_Date(complaintAttempt_DateTime,'yyyyMMddHH24MIss') between To_Date('" + TimeFrom + "','yyyyMMddHH24MIss') and To_Date('" + TimeTo + "','yyyyMMddHH24MIss') AND  To_Date(Status_DateTime,'yyyyMMddHH24MIss') between To_Date('" + TimeFrom + "','yyyyMMddHH24MIss') and To_Date('" + TimeTo + "','yyyyMMddHH24MIss') ) ) and MTL is not NULL and LineMan_Name is not null " +
                                               "   order by crmserviceticket_no ASC";

                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }
        #endregion

        #region  MTL KPI...

        public DataTable GetMTLKPIShiftDetail(string DateFrom, string DateTo, string QueryString,string IBCCODE="")
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "" && QueryString != null)
                {
                    if (IBCCODE != "" && IBCCODE !=null)
                    _commnadData.CommandText = "select  To_Char(ShiftDate,'dd-MON-yy') AS ShiftDate,D.ShiftName,DT.MTL,DT.GangName Team,ShiftStartTime,ShiftEndTime from DutyRooster D   " +
                                                  " inner join DutyRoosterTeam DT on D.DUtyroosterid=DT.DutyRoosterid     inner join centre C on D.Centre=C.MNCName and trim(C.Mnccode) in (" + IBCCODE + ")      " +
                                                  " where shiftsignoutTime is not null and ShiftSignInTime is not null           " +
                                             "  AND To_Date(To_Char(ShiftDate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') " +
                                               " between  To_Date('" + DateFrom + "','yyyyMMddHH24MIss') AND      To_Date('" + DateTo + "','yyyyMMddHH24MIss')" +
                                               " AND " + QueryString + "    Order By DT.Dutyroosterteamid ASC   ";
                    else
                        _commnadData.CommandText = "select  To_Char(ShiftDate,'dd-MON-yy') AS ShiftDate,D.ShiftName,DT.MTL,DT.GangName Team,ShiftStartTime,ShiftEndTime from DutyRooster D   " +
                                                 " inner join DutyRoosterTeam DT on D.DUtyroosterid=DT.DutyRoosterid         " +
                                                 " where shiftsignoutTime is not null and ShiftSignInTime is not null           " +
                                            "  AND To_Date(To_Char(ShiftDate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') " +
                                              " between  To_Date('" + DateFrom + "','yyyyMMddHH24MIss') AND      To_Date('" + DateTo + "','yyyyMMddHH24MIss')" +
                                              " AND " + QueryString + "    Order By DT.Dutyroosterteamid ASC   ";

                }
                else
                {
                    if (IBCCODE != "" && IBCCODE != null)
                    {
                        _commnadData.CommandText = "select  To_Char(ShiftDate,'dd-MON-yy') AS ShiftDate,D.ShiftName,DT.MTL,DT.GangName Team,ShiftStartTime,ShiftEndTime from DutyRooster D   " +
                                                  " inner join DutyRoosterTeam DT on D.DUtyroosterid=DT.DutyRoosterid  inner join centre C on D.Centre=C.MNCName and trim(C.Mnccode) in (" + IBCCODE + ")          " +
                                                  " where shiftsignoutTime is not null and ShiftSignInTime is not null           " +
                                             "  AND To_Date(To_Char(ShiftDate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') " +
                                               " between  To_Date('" + DateFrom + "','yyyyMMddHH24MIss') AND      To_Date('" + DateTo + "','yyyyMMddHH24MIss')" +
                                               "  Order By DT.Dutyroosterteamid ASC   ";
                    }
                    else
                    {
                        _commnadData.CommandText = "select  To_Char(ShiftDate,'dd-MON-yy') AS ShiftDate,D.ShiftName,DT.MTL,DT.GangName Team,ShiftStartTime,ShiftEndTime from DutyRooster D   " +
                                                 " inner join DutyRoosterTeam DT on D.DUtyroosterid=DT.DutyRoosterid         " +
                                                 " where shiftsignoutTime is not null and ShiftSignInTime is not null           " +
                                            "  AND To_Date(To_Char(ShiftDate,'yyyyMMddHH24MIss'),'yyyyMMddHH24MIss') " +
                                              " between  To_Date('" + DateFrom + "','yyyyMMddHH24MIss') AND      To_Date('" + DateTo + "','yyyyMMddHH24MIss')" +
                                              "  Order By DT.Dutyroosterteamid ASC   ";
                    }
                }



                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }
        public DataTable GetMTLKPIDistanceByShiftLineManMTL(string DateFrom, string DateTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "" && QueryString != null)
                {

                    _commnadData.CommandText = " select To_Date(RecordDate,'yyyyMMddHH24MIss') RecordDateTime,MTL.* from MTLKPIDistance MTL" +

                                                 " where To_Date(RecordDate,'yyyyMMddHH24MIss')  between  To_Date('" + DateFrom + "','yyyyMMddHH24MIss') AND      To_Date('" + DateTo + "','yyyyMMddHH24MIss')" +
                                               "  AND " + QueryString;
                }
                else
                {

                    _commnadData.CommandText = " select To_Date(RecordDate,'yyyyMMddHH24MIss') RecordDateTime,MTL.* from MTLKPIDistance MTL " +

                                                 " where To_Date(RecordDate,'yyyyMMddHH24MIss')  between  To_Date('" + DateFrom + "','yyyyMMddHH24MIss') AND      To_Date('" + DateTo + "','yyyyMMddHH24MIss')";

                }



                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }
        #endregion


        #region Monthly Complaint Record for Export to Excel


        public DataTable GetMonthlyComplaintRecordByDate(string DateFrom, string DateTo, string TimeFrom, string TimeTo, string QueryString)
        {
            //Creating object of DAL class
            CommandData _commnadData = new CommandData();

            try
            {
                _commnadData._CommandType = CommandType.Text;
                if (QueryString != "")
                {



                    string Query = "   SELECT  COMPLAINTID,To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'MM/dd/yyyy') AS\" Complaint Date\",REOPENCOUNT AS\"Re-Open\",CRMSERVICETICKETNO as\"CRM No\",BPID AS \"BP NO\",MOBILENO AS \"Mobile No\",TELEPHONENO  AS \"PTCL NO\",ConsumerName AS \"Name\",Address,MNCCODE AS\"DIV\",LINEMANNAME AS \"LineMan Name\",To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24:MI')  AS \" Recieve Time\",To_Char(To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss'),'HH24:MI')  AS \" Attempt Time\",COMPLAINTTYPECODE  AS \"Complaint Type Name\",STATUSTO AS \"Status To\",STATUSREASON as \"Status Reason\",INTERNALNODE AS \"Internal Note\",DTSID AS \"DTS ID\",PMTNAME AS \"PMT Name\",NVL(PMTVERIFIED,'0') AS \"Mapping\",FEEDERNAME As \"Feeder Name\",CLINO,CONTRACTACCOUNTNO,CONTRACTNO,RECHECKCOUNT,PARENTTICKETNO,COMPLAINTTYPE,REASONOFFAULT,SUPERVISORNAME AS \"Super Visor Name\",MTL,TIMESLICE,NVL(To_Char(To_Date(COMPLAINTATTEMPTDATETIME,'yyyyMMddHH24MIss'),'MM/dd/yyyy'),'') AS \"Complaint Attempt Date\",FEEDERID,STATUS,NVL( To_Char(To_Date(STATUSDATETIME,'yyyyMMddHH24MIss'),'MM/dd/yyyy'),'') AS \"Status Date\",IBCCODE,COMPLAINTLAT,COMPLAINTLON,Zone,STREET,FLAG,MANUALTICKET,CAREOF,TOTALCHILDCOUNT,VLAT,VLON,WIRESIZE,WIREREQUIRED " +
                                  "  FROM COMPLAINT C   " +
                                  "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null " +
                                  "   AND  " +

                                  "  ( " +
                                   "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                    " ) " +
                                    " AND   " + QueryString;

                    //string Query = "   SELECT  To_Date( SUBSTR(ComplaintDateTime,0,8),'yyyyMMdd') ReceiveDate,NVL(To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24Miss'),'') AttempDateTime,trim(ReOpenCount) ReOpenCount,CRMSERVICETICKETNO,BPID,TelephoneNo,MobileNo,ConsumerName ,Address ,Zone ,LineManName ,SUBSTR(ComplaintDateTime,9,2) ||':'|| SUBSTR(ComplaintDateTime,11,2) RecTime, SUBSTR(ComplaintAttemptDateTime,9,2) ||':'|| SUBSTR(ComplaintAttemptDateTime,11,2) AttendTime, ComplaintTypeCode,StatusTo,statusReason,Internalnode,pmtName,FeederName,DTSID " +
                    //              "  FROM COMPLAINT C   " +
                    //              "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null " +
                    //              "   AND  " +

                    //              "  ( " +
                    //               "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                    //                " ) " +
                    //                " AND   " + QueryString;

                    

                    
                        _commnadData.CommandText = Query;



                }
                else
                {

                    //string Query = "   SELECT  COMPLAINTID,CRMSERVICETICKETNO,MOBILENO,TELEPHONENO,CLINO,COMPLAINTDATETIME,CONTRACTACCOUNTNO,CONTRACTNO,CONSUMERNAME,REOPENCOUNT,BPID,ADDRESS,RECHECKCOUNT,PARENTTICKETNO,COMPLAINTTYPE,REASONOFFAULT,INTERNALNODE,LINEMANNAME,SUPERVISORNAME,MTL,PMTNAME,DTSID,COMPLAINTATTEMPTDATETIME,TIMESLICE,STATUSTO,STATUSREASON,FEEDERNAME,FEEDERID,STATUS,STATUSDATETIME,IBCCODE,COMPLAINTLAT,COMPLAINTLON,MNCCODE,STREET,ZONE,COMPLAINTTYPECODE,FLAG,MANUALTICKET,CAREOF,PMTVERIFIED,TOTALCHILDCOUNT,VLAT,VLON,WIRESIZE,WIREREQUIRED " +

                    //           "  FROM COMPLAINT C   " +
                    //           "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null " +
                    //           "   AND  " +

                    //           "  ( " +
                    //            "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                    //             " ) ";
                    string Query = "   SELECT  COMPLAINTID,To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'MM/dd/yyyy')  AS\" Complaint Date\",REOPENCOUNT AS\"Re-Open\",CRMSERVICETICKETNO as\"CRM No\",BPID AS \"BP NO\",MOBILENO AS \"Mobile No\",TELEPHONENO  AS \"PTCL NO\",ConsumerName AS \"Name\",Address,MNCCODE AS\"DIV\" ,LINEMANNAME AS \"LineMan Name\",To_Char(To_Date(ComplaintDateTime,'yyyyMMddHH24MIss'),'HH24:MI')  AS \" Recieve Time\",To_Char(To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24MIss'),'HH24:MI')  AS \" Attempt Time\",COMPLAINTTYPECODE  AS \"Complaint Type Name\",STATUSTO AS \"Status To\",STATUSREASON as \"Status Reason\",INTERNALNODE AS \"Internal Note\",DTSID AS \"DTS ID\",PMTNAME AS \"PMT Name\",NVL(PMTVERIFIED,'0') AS \"Mapping\",FEEDERNAME As \"Feeder Name\",CLINO,CONTRACTACCOUNTNO,CONTRACTNO,RECHECKCOUNT,PARENTTICKETNO,COMPLAINTTYPE,REASONOFFAULT,SUPERVISORNAME AS \"Super Visor Name\",MTL,TIMESLICE,NVL( To_Char(To_Date(COMPLAINTATTEMPTDATETIME,'yyyyMMddHH24MIss'),'MM/dd/yyyy'),'') AS \"Complaint Attempt Date\" ,FEEDERID,STATUS,NVL( To_Char(To_Date(STATUSDATETIME,'yyyyMMddHH24MIss'),'MM/dd/yyyy'),'') AS \"Status Date\",IBCCODE,COMPLAINTLAT,COMPLAINTLON,Zone,STREET,FLAG,MANUALTICKET,CAREOF,TOTALCHILDCOUNT,VLAT,VLON,WIRESIZE,WIREREQUIRED " +
                                 "  FROM COMPLAINT C   " +
                                 "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null " +
                                 "   AND  " +

                                 "  ( " +
                                  "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                                   " ) ";
                    //string Query = "   SELECT  To_Date( SUBSTR(ComplaintDateTime,0,8),'yyyyMMdd') ReceiveDate,NVL(To_Date(ComplaintAttemptDateTime,'yyyyMMddHH24Miss'),'') AttemptDateTime,trim(ReOpenCount) ReOpenCount,CRMSERVICETICKETNO,BPID,TelephoneNo,MobileNo,ConsumerName ,Address ,Zone ,LineManName ,SUBSTR(ComplaintDateTime,9,2) ||':'|| SUBSTR(ComplaintDateTime,11,2) RecTime, SUBSTR(ComplaintAttemptDateTime,9,2) ||':'|| SUBSTR(ComplaintAttemptDateTime,11,2) AttendTime, ComplaintTypeCode,StatusTo,statusReason,Internalnode,pmtName,FeederName,DTSID " +
                    //                "  FROM COMPLAINT C   " +
                    //                "   WHERE  trim(ComplaintDateTime) !='0'  and ComplaintDateTime is not null " +
                    //                "   AND  " +

                    //                "  ( " +
                    //                 "  To_Date(ComplaintDateTime,'yyyyMMddHH24MIss') between  To_Date('" + TimeFrom + "','yyyyMMddHH24MIss')   AND    To_Date('" + TimeTo + "','yyyyMMddHH24MIss') " +
                    //                  " ) ";
                                     


                    _commnadData.CommandText = Query;
                }


                //Adding Parameters
                // _commnadData.AddParameter("@UserName", userID);

                //opening connection
                _commnadData.OpenWithOutTrans();

                //Executing Query
                DataSet _ds = _commnadData.Execute(ExecutionType.ExecuteDataSet) as DataSet;

                return _ds.Tables[0];
            }
            catch (Exception ex)
            {
                //Console.WriteLine("No record found");
                throw ex;
            }
            finally
            {
                //Console.WriteLine("No ");
                _commnadData.Close();

            }
        }

        #endregion

    }
}
