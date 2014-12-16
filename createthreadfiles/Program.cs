using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Data;


namespace createthreadfiles
{
    class Program
    {
        static void Main(string[] args)
        {
            string sNum = args[0].ToString();
            int threadcount = Convert.ToInt32(sNum);

            Console.Write("Processing File Count : " + sNum);
            CreateThreadFiles(threadcount);

        }

        private static void CreateThreadFiles(int numThreads)
        {
           // InitializeAWS();

            Console.Write("Entering Code");
            string connStr = "Data Source=216.139.239.77;Initial Catalog=MSUPC;Persist Security Info=True;User ID=MS_APP;Password=D1am0nDcutt3r";
            bool ok = false;
            string threadLocFile = @"D:\MediaSpider\ExternalThreads\";
            while (!ok)
            {
                string asin = "";
                try
                {
                    //*********************************************************************************************************************
                    //TODO: REMOVE START AND END PARAMETERS ForThreadFile_ssp
                    //*********************************************************************************************************************
                    //int startCode = 0;
                   // int endCode = 99999999;

                    FileStream fs = new FileStream(@threadLocFile + "\\miscthreads.txt", FileMode.Create, FileAccess.ReadWrite);
                    StreamWriter sw = new StreamWriter(fs);
                    sw.AutoFlush = true;

                    // sets up a connection to the SQL database
                    SqlConnection conn = new SqlConnection(connStr);
                    SqlConnection connInsert = new SqlConnection(connStr);
                    //swLog.WriteLine(searchIndex + locale + "ForThreadFile_ssp");
                    // (Products X ForThreadFile_ssp)
                    // Selects ASIN values from the product table
                    SqlCommand cmd = new SqlCommand("MWS_Ext_ProductUSForThreadFile_ssp", conn);
                    cmd.CommandType = CommandType.StoredProcedure;
                   // cmd.Parameters.Add("@IDStart", SqlDbType.Int);
                    //cmd.Parameters.Add("@IDEnd", SqlDbType.Int);
                   // cmd.Parameters[0].Value = startCode;
                    //cmd.Parameters[1].Value = endCode;
                    cmd.CommandTimeout = 6000;

                    // open the connection to the database if it is not already open
                    if (conn.State != ConnectionState.Open)
                        conn.Open();

                    // execute the SQL command ProductsUSForThreadFile_ssp
                    // and return the ASIN values from the product table.
                    // Save the values to r

                    Console.Write("Executing SQL Reader");
                    SqlDataReader r;
                    r = cmd.ExecuteReader();

                    // if there are any more values returned from the SQL command,
                    // set the variable asin to the first 10 characters of the returned ASIN value
                    if (r.Read())
                        asin = r.GetString(0).Substring(0, 10);

                    // set up an ineger to store the number of values returned from the SQL command
                    int recordCount = 0;
                    // write the first ten characters of all of the returned ASIN values to the stream
                    while ((r.Read()))
                    {
                        sw.WriteLine(asin);
                        recordCount++;
                        asin = r.GetString(0).Substring(0, 10);
                    }
                    // close the open streams and sqldatareader
                    r.Close();
                    sw.Close();
                    fs.Close();


                    Console.Write("Done Reading Initial File Records: " + recordCount.ToString());
                    // close the sql connection if it hasn't aready been closed
                    if (conn.State != ConnectionState.Closed)
                        conn.Close();

                    Console.Write("Entering Thread File cReationg");
                    // creates a reader for the text file that the asin values have been writen to
                    StreamReader sr = new StreamReader(@threadLocFile + "\\miscthreads.txt");

                    // initialize a new stream writer array with the length of the parameter numThreads
                    StreamWriter[] swArray = new StreamWriter[numThreads];

                    // set the location of the stream for each value in the array
                    for (int i = 0; i < numThreads; i++)
                    {
                        swArray[i] = new StreamWriter(@threadLocFile + "\\outputfiles\\thread" +  (i + 1).ToString() + ".txt");
                    }

                    sr.BaseStream.Seek(0, SeekOrigin.Begin);

                    // sets the asin value to the first ASIN value in the newly writen text file
                    asin = sr.ReadLine();
                    // as long as there are more asin values in the text file,
                    // cycle through the array stream writing each of the values in the array to the next value in the text file
                    while (asin != null)
                    {
                        for (int i = 0; i < numThreads; i++)
                        {
                            if (asin != null)
                            {
                                swArray[i].WriteLine(asin);
                                asin = sr.ReadLine();
                            }
                        }
                    }//end while

                    // close the stream reader and each value in the stream reader
                    sr.Close();
                    for (int i = 0; i < numThreads; i++)
                    {
                        swArray[i].Close();
                    }
                    // break the loop and end method
                    ok = true;
                }
                // write any issues that may have occured to the log file
                catch (Exception ex)
                {
                    Console.Write("Exception in Thread file:" + ex.Message);
                    System.Threading.Thread.Sleep(10000);
                   // swLog.WriteLine("ERROR: CreateThreadFiles - " + ex.Message);
                }
            }//end while
           // swLog.WriteLine();
        }

    }
}
