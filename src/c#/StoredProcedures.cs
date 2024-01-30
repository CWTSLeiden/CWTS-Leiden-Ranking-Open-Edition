using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections.Generic;


public partial class StoredProcedures
{
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void CalcStabilityIntervals(SqlDouble coverageProb, SqlInt32 nBootstrapSamples)
    {
        int i, j, k, l, m, n, p, o;
        
        List<int> pubSetNo1 = new List<int>();
        List<double> weight = new List<double>();
        List<double[]> indicator = new List<double[]>();
        int nIndicators = -1;
        string[] indicatorName = null;
        using (var connection = new SqlConnection("context connection=true"))
        {
            connection.Open();
            using (SqlCommand command = connection.CreateCommand())
            {
                command.CommandText = "select * from #bootstrap_input order by pub_set_no";
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    nIndicators = reader.FieldCount - 2;
                    indicatorName = new string[nIndicators];
                    for (i = 0; i < nIndicators; i++)
                        indicatorName[i] = reader.GetName(i + 2);
                    while (reader.Read())
                    {
                        pubSetNo1.Add(reader.GetInt32(0));
                        weight.Add(reader.GetDouble(1));
                        double[] indicator2 = new double[nIndicators];
                        for (i = 0; i < nIndicators; i++)
                            indicator2[i] = reader.GetDouble(i + 2);
                        indicator.Add(indicator2);
                    }
                }
            }
            connection.Close();
        }

        SqlContext.Pipe.Send("Number of publications: " + pubSetNo1.Count);

        List<int> pubSetNo2 = new List<int>();
        List<double[]> average = new List<double[]>();
        List<double[]> lowerBound = new List<double[]>();
        List<double[]> upperBound = new List<double[]>();
        double[,] indicatorMean = new double[nIndicators, nBootstrapSamples.Value];
        double[] indicatorMean2 = new double[nBootstrapSamples.Value];
        int lowerBoundIndex = (int)Math.Floor(((1 - coverageProb.Value) / 2) * nBootstrapSamples.Value);
        int upperBoundIndex = (int)Math.Floor((1 - (1 - coverageProb.Value) / 2) * nBootstrapSamples.Value);
        i = 0;
        Random random = new Random();
        while (i < pubSetNo1.Count)
        {
            j = pubSetNo1[i];
            k = i + 1;
            while ((k < pubSetNo1.Count) && (pubSetNo1[k] == j))
                k++;

            pubSetNo2.Add(j);

            double weightSum = 0;
            double[] indicatorSum = new double[nIndicators];
            for (l = i; l < k; l++)
            {
                weightSum += weight[l];
                for (m = 0; m < nIndicators; m++)
                    indicatorSum[m] += weight[l] * indicator[l][m];
            }
            double[] average2 = new double[nIndicators];
            for (l = 0; l < nIndicators; l++)
                average2[l] = indicatorSum[l] / weightSum;
            average.Add(average2);

            l = k - i;
            for (m = 0; m < nBootstrapSamples; m++)
            {
                weightSum = 0;
                indicatorSum = new double[nIndicators];
                for (n = 0; n < l; n++)
                {
                    o = i + random.Next(l);
                    weightSum += weight[o];
                    for (p = 0; p < nIndicators; p++)
                        indicatorSum[p] += weight[o] * indicator[o][p];
                }
                for (n = 0; n < nIndicators; n++)
                    indicatorMean[n, m] = indicatorSum[n] / weightSum;
            }

            double[] lowerBound2 = new double[nIndicators];
            double[] upperBound2 = new double[nIndicators];
            for (m = 0; m < nIndicators; m++)
            {
                for (n = 0; n < nBootstrapSamples; n++)
                    indicatorMean2[n] = indicatorMean[m, n];
                Array.Sort(indicatorMean2);
                lowerBound2[m] = indicatorMean2[lowerBoundIndex];
                upperBound2[m] = indicatorMean2[upperBoundIndex];
            }
            lowerBound.Add(lowerBound2);
            upperBound.Add(upperBound2);

            i = k;
        }

        SqlMetaData[] columnMetaData = new SqlMetaData[3 * nIndicators + 1];
        columnMetaData[0] = new SqlMetaData("pub_set_no", SqlDbType.Int);
        for (i = 0; i < nIndicators; i++)
        {
            columnMetaData[3 * i + 1] = new SqlMetaData(indicatorName[i] + "_avg", SqlDbType.Float);
            columnMetaData[3 * i + 2] = new SqlMetaData(indicatorName[i] + "_lb", SqlDbType.Float);
            columnMetaData[3 * i + 3] = new SqlMetaData(indicatorName[i] + "_ub", SqlDbType.Float);
        }
        SqlDataRecord record = new SqlDataRecord(columnMetaData);
        SqlContext.Pipe.SendResultsStart(record);
        for (i = 0; i < pubSetNo2.Count; i++)
        {
            record.SetValue(0, pubSetNo2[i]);
            for (j = 0; j < nIndicators; j++)
            {
                record.SetValue(3 * j + 1, average[i][j]);
                record.SetValue(3 * j + 2, lowerBound[i][j]);
                record.SetValue(3 * j + 3, upperBound[i][j]);
            }
            SqlContext.Pipe.SendResultsRow(record);
        }
        SqlContext.Pipe.SendResultsEnd();
    }
};
