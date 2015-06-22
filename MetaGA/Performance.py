__author__ = 'Ciddhi'

from datetime import timedelta, datetime
import csv
import GlobalVariables as gv
from DBUtils import *

class Performance:

    def calculatePerformancePortfolio(self, startDate, endDate, portfolioId, dbObject):
        resultDates = dbObject.dbQuery("SELECT DISTINCT(PriceDate), 1 FROM tblStockPriceData WHERE PriceDate >= '" + str(startDate)+
                                       "' AND PriceDate <= '"+str(endDate)+"'")
        DictOfDates={}
        date_count_range=0
        for date,dummy in resultDates:
            DictOfDates[date]=date_count_range
            date_count_range=date_count_range+1

        query = "SELECT * FROM portfolio_tradesheet_data_table WHERE EntryDate >= '" + str(startDate) + "' AND EntryDate <= '" \
               + str(endDate) + "' AND MetaIndividualId=" + str(portfolioId)

        resultTrades = dbObject.dbQuery(query)
        c=0
        #Parameters for each individual
        Performance_Measures=[] #List of Tuples to store the Performance_Measures the calculation_type
        #Lists to store the variables for daily calculations
        tmp_daily_running_pl=[]
        #Variables for the total period of between begin date and end date
        total_Win_Long_Trades=0
        total_Win_Short_Trades=0
        total_Loss_Long_Trades=0
        total_Loss_Short_Trades=0
        total_Profit_Long=0
        total_Profit_Short=0
        total_Loss_Long=0
        total_Loss_Short=0

        #Creating a list which stores daily PL values
        for filler_date_count in range(date_count_range):
            tmp_daily_running_pl.append(0)
        date_count=0
        diff=0
        DD_History = [] #List to store the DD values.
        Gain_History = []

        for meta_id, stock_id, individual_id, trade_entry_date, trade_entry_time, trade_entry_price, trade_exit_date, trade_exit_time, trade_exit_price, trade_qty, trade_type  in resultTrades:
            individual_id = portfolioId
            CurrentDate=trade_entry_date
            if(c==0):
                prev_trade_entry_date=trade_entry_date
            elif(prev_individual_id==individual_id):
                diff=1
            else:
                if(tmp_daily_running_pl[0] < 0):
                    DD_History.append(tmp_daily_running_pl[0])
                else:
                    DD_History.append(0)
                DD_date_count = 1
                while(DD_date_count < date_count_range):
                    if(tmp_daily_running_pl[DD_date_count]<0):
                        DD_History.append(DD_History[DD_date_count-1]+tmp_daily_running_pl[DD_date_count])
                    else:
                        DD_History.append(0)
                    DD_date_count = DD_date_count +1
                total_DD=0
                for DD_Daily_Value in DD_History:
                    if(DD_Daily_Value<total_DD):
                        total_DD=DD_Daily_Value
                TotalTrades= total_Win_Short_Trades+total_Loss_Short_Trades+total_Win_Long_Trades+total_Loss_Long_Trades
                if(TotalTrades==0 or total_DD==0):
                    Performance_Measures.append((gv.dummyPerformance,gv.dummyPerformance, 0, gv.dummyPerformance, gv.dummyPerformance, 0, 0.0))
                else:
                    if(tmp_daily_running_pl[0]>0):
                        Gain_History.append(tmp_daily_running_pl[0])
                    else:
                        Gain_History.append(0)
                    Gain_date_count=1
                    while(Gain_date_count <date_count_range):
                        if(tmp_daily_running_pl[Gain_date_count]>0):
                            Gain_History.append(Gain_History[Gain_date_count-1]+tmp_daily_running_pl[Gain_date_count])
                        else:
                            Gain_History.append(0)
                        Gain_date_count+=1
                    total_Gain=0.0
                    for Gain_Daily_Value in Gain_History:
                        if(Gain_Daily_Value>total_Gain):
                            total_Gain=Gain_Daily_Value
                    NetPL=total_Profit_Long+total_Loss_Long+total_Profit_Short+total_Loss_Short
                    ProfitMakingEpochs=1.0*(total_Win_Long_Trades+total_Win_Short_Trades)/(1.0*(TotalTrades))
                    Performance_Measures.append((NetPL/(TotalTrades), NetPL/(-total_DD), total_Gain, total_DD, NetPL, TotalTrades, ProfitMakingEpochs))

                Performance_Measures=[] #List of Tuples to store the Performance_Measures the calculation_type
                #Lists to store the variables for daily calculations
                tmp_daily_running_pl=[]
                #Variables for the total period of between begin date and end date
                total_Win_Long_Trades=0
                total_Win_Short_Trades=0
                total_Loss_Long_Trades=0
                total_Loss_Short_Trades=0
                total_Profit_Long=0
                total_Profit_Short=0
                total_Loss_Long=0
                total_Loss_Short=0
                for filler_date_count in range(date_count_range):
                    tmp_daily_running_pl.append(0)

                date_count=0
                diff=0
                DD_History = [] #List to store the DD values.
                Gain_History = [] #List to store the MaxGain values
                IndividualIDExist=0

            if(DictOfDates.has_key(CurrentDate)):
                date_count= DictOfDates[CurrentDate]
                if(trade_type==1):
                    trade_pl = (trade_exit_price - trade_entry_price)*trade_qty
                    if(trade_pl>0):
                        total_Win_Long_Trades+=1
                        total_Profit_Long+=trade_pl
                    else:
                        total_Loss_Long_Trades+=1
                        total_Loss_Long+=trade_pl
                else:
                    trade_pl = (trade_entry_price - trade_exit_price)*trade_qty
                    if(trade_pl>0):
                        total_Win_Short_Trades+=1
                        total_Profit_Short+=trade_pl
                    else:
                        total_Loss_Short_Trades+=1
                        total_Loss_Short+=trade_pl

                tmp_daily_running_pl[date_count] = tmp_daily_running_pl[date_count]+trade_pl
            c=1
            prev_individual_id=individual_id
            prev_trade_entry_date=trade_entry_date
        if(tmp_daily_running_pl[0] < 0):
            DD_History.append(tmp_daily_running_pl[0])
        else:
            DD_History.append(0)
        DD_date_count = 1
        while(DD_date_count < date_count_range):
            if(tmp_daily_running_pl[DD_date_count]<0):
                DD_History.append(DD_History[DD_date_count-1]+tmp_daily_running_pl[DD_date_count])
            else:
                DD_History.append(0)
            DD_date_count = DD_date_count +1
        total_DD=0
        for DD_Daily_Value in DD_History:
            if(DD_Daily_Value<total_DD):
                total_DD=DD_Daily_Value

        TotalTrades= total_Win_Short_Trades+total_Loss_Short_Trades+total_Win_Long_Trades+total_Loss_Long_Trades

        if(TotalTrades==0 or total_DD==0):
            Performance_Measures.append((gv.dummyPerformance,gv.dummyPerformance, 0, gv.dummyPerformance, gv.dummyPerformance, 0, 0.0))
        else:
            #calculation of MaxGain (Similar to DD Calculation):
            if(tmp_daily_running_pl[0]>0):
                Gain_History.append(tmp_daily_running_pl[0])
            else:
                Gain_History.append(0)
            Gain_date_count=1
            while(Gain_date_count <date_count_range):
                if(tmp_daily_running_pl[Gain_date_count]>0):
                    Gain_History.append(Gain_History[Gain_date_count-1]+tmp_daily_running_pl[Gain_date_count])
                else:
                    Gain_History.append(0)
                Gain_date_count+=1
            total_Gain=0.0
            for Gain_Daily_Value in Gain_History:
                if(Gain_Daily_Value>total_Gain):
                    total_Gain=Gain_Daily_Value

            NetPL=total_Profit_Long+total_Loss_Long+total_Profit_Short+total_Loss_Short
            ProfitMakingEpochs=1.0*(total_Win_Long_Trades+total_Win_Short_Trades)/(1.0*(TotalTrades))
            Performance_Measures.append((NetPL/(TotalTrades), NetPL/(-total_DD), total_Gain, total_DD, NetPL, TotalTrades, ProfitMakingEpochs))
        return Performance_Measures

    def calculateReferencePerformanceIndividual(self, individualId, stockId,  startDate, endDate, dbObject):
        resultDates = dbObject.dbQuery("SELECT DISTINCT(PriceDate), 1 FROM tblStockPriceData WHERE PriceDate >= '" + str(startDate)+
                                       "' AND PriceDate <= '"+str(endDate)+"'")
        DictOfDates={}
        date_count_range=0
        for date,dummy in resultDates:
            DictOfDates[date]=date_count_range
            date_count_range=date_count_range+1
        query = "SELECT * FROM tblIndividualTradesheet WHERE EntryDate >= '" + str(startDate) + "' AND EntryDate <= '" \
               + str(endDate) + "' AND IndividualID=" + str(individualId) + " AND SecID=" + str(stockId)
        resultTrades = dbObject.dbQuery(query)
        c=0
        #Parameters for each individual
        Performance_Measures=[] #List of Tuples to store the Performance_Measures the calculation_type
        #Lists to store the variables for daily calculations
        tmp_daily_running_pl=[]
        #Variables for the total period of between begin date and end date
        total_Win_Long_Trades=0
        total_Win_Short_Trades=0
        total_Loss_Long_Trades=0
        total_Loss_Short_Trades=0
        total_Profit_Long=0
        total_Profit_Short=0
        total_Loss_Long=0
        total_Loss_Short=0

        #Creating a list which stores daily PL values
        for filler_date_count in range(date_count_range):
            tmp_daily_running_pl.append(0)
        date_count=0
        diff=0
        DD_History = [] #List to store the DD values.
        Gain_History = []

        for stock_id, individual_id, trade_entry_date, trade_entry_time, trade_entry_price, trade_exit_date, trade_exit_time, trade_exit_price, trade_qty, trade_type in resultTrades:
            CurrentDate=trade_entry_date
            if(c==0):
                prev_trade_entry_date=trade_entry_date
            elif(prev_individual_id==individual_id):
                diff=1
            else:
                if(tmp_daily_running_pl[0] < 0):
                    DD_History.append(tmp_daily_running_pl[0])
                else:
                    DD_History.append(0)
                DD_date_count = 1
                while(DD_date_count < date_count_range):
                    if(tmp_daily_running_pl[DD_date_count]<0):
                        DD_History.append(DD_History[DD_date_count-1]+tmp_daily_running_pl[DD_date_count])
                    else:
                        DD_History.append(0)
                    DD_date_count = DD_date_count +1
                total_DD=0
                for DD_Daily_Value in DD_History:
                    if(DD_Daily_Value<total_DD):
                        total_DD=DD_Daily_Value
                TotalTrades= total_Win_Short_Trades+total_Loss_Short_Trades+total_Win_Long_Trades+total_Loss_Long_Trades
                if(TotalTrades==0 or total_DD==0):
                    Performance_Measures.append((gv.dummyPerformance,gv.dummyPerformance, 0, gv.dummyPerformance, gv.dummyPerformance, 0, 0.0))
                else:
                    if(tmp_daily_running_pl[0]>0):
                        Gain_History.append(tmp_daily_running_pl[0])
                    else:
                        Gain_History.append(0)
                    Gain_date_count=1
                    while(Gain_date_count <date_count_range):
                        if(tmp_daily_running_pl[Gain_date_count]>0):
                            Gain_History.append(Gain_History[Gain_date_count-1]+tmp_daily_running_pl[Gain_date_count])
                        else:
                            Gain_History.append(0)
                        Gain_date_count+=1
                    total_Gain=0.0
                    for Gain_Daily_Value in Gain_History:
                        if(Gain_Daily_Value>total_Gain):
                            total_Gain=Gain_Daily_Value
                    NetPL=total_Profit_Long+total_Loss_Long+total_Profit_Short+total_Loss_Short
                    ProfitMakingEpochs=1.0*(total_Win_Long_Trades+total_Win_Short_Trades)/(1.0*(TotalTrades))
                    Performance_Measures.append((NetPL/(TotalTrades), NetPL/(-total_DD), total_Gain, total_DD, NetPL, TotalTrades, ProfitMakingEpochs))

                Performance_Measures=[] #List of Tuples to store the Performance_Measures the calculation_type
                #Lists to store the variables for daily calculations
                tmp_daily_running_pl=[]
                #Variables for the total period of between begin date and end date
                total_Win_Long_Trades=0
                total_Win_Short_Trades=0
                total_Loss_Long_Trades=0
                total_Loss_Short_Trades=0
                total_Profit_Long=0
                total_Profit_Short=0
                total_Loss_Long=0
                total_Loss_Short=0
                for filler_date_count in range(date_count_range):
                    tmp_daily_running_pl.append(0)

                date_count=0
                diff=0
                DD_History = [] #List to store the DD values.
                Gain_History = [] #List to store the MaxGain values
                IndividualIDExist=0

            if(DictOfDates.has_key(CurrentDate)):
                date_count= DictOfDates[CurrentDate]
                if(trade_type==1):
                    trade_pl = (trade_exit_price - trade_entry_price)*trade_qty
                    if(trade_pl>0):
                        total_Win_Long_Trades+=1
                        total_Profit_Long+=trade_pl
                    else:
                        total_Loss_Long_Trades+=1
                        total_Loss_Long+=trade_pl
                else:
                    trade_pl = (trade_entry_price - trade_exit_price)*trade_qty
                    if(trade_pl>0):
                        total_Win_Short_Trades+=1
                        total_Profit_Short+=trade_pl
                    else:
                        total_Loss_Short_Trades+=1
                        total_Loss_Short+=trade_pl

                tmp_daily_running_pl[date_count] = tmp_daily_running_pl[date_count]+trade_pl
            c=1
            prev_individual_id=individual_id
            prev_trade_entry_date=trade_entry_date
        if(tmp_daily_running_pl[0] < 0):
            DD_History.append(tmp_daily_running_pl[0])
        else:
            DD_History.append(0)
        DD_date_count = 1
        while(DD_date_count < date_count_range):
            if(tmp_daily_running_pl[DD_date_count]<0):
                DD_History.append(DD_History[DD_date_count-1]+tmp_daily_running_pl[DD_date_count])
            else:
                DD_History.append(0)
            DD_date_count = DD_date_count +1
        total_DD=0
        for DD_Daily_Value in DD_History:
            if(DD_Daily_Value<total_DD):
                total_DD=DD_Daily_Value

        TotalTrades= total_Win_Short_Trades+total_Loss_Short_Trades+total_Win_Long_Trades+total_Loss_Long_Trades

        if(TotalTrades==0 or total_DD==0):
            Performance_Measures.append((gv.dummyPerformance, gv.dummyPerformance, 0, gv.dummyPerformance, gv.dummyPerformance, 0, 0.0))
        else:
            #calculation of MaxGain (Similar to DD Calculation):
            if(tmp_daily_running_pl[0]>0):
                Gain_History.append(tmp_daily_running_pl[0])
            else:
                Gain_History.append(0)
            Gain_date_count=1
            while(Gain_date_count <date_count_range):
                if(tmp_daily_running_pl[Gain_date_count]>0):
                    Gain_History.append(Gain_History[Gain_date_count-1]+tmp_daily_running_pl[Gain_date_count])
                else:
                    Gain_History.append(0)
                Gain_date_count+=1
            total_Gain=0.0
            for Gain_Daily_Value in Gain_History:
                if(Gain_Daily_Value>total_Gain):
                    total_Gain=Gain_Daily_Value

            NetPL=total_Profit_Long+total_Loss_Long+total_Profit_Short+total_Loss_Short
            ProfitMakingEpochs=1.0*(total_Win_Long_Trades+total_Win_Short_Trades)/(1.0*(TotalTrades))
            Performance_Measures.append((NetPL/(TotalTrades), NetPL/(-total_DD), total_Gain, total_DD, NetPL, TotalTrades, ProfitMakingEpochs))
        return Performance_Measures

    def calculatePerformancePortfolioList(self, startDate, endDate, portfolioList, dbObject):
        resultDates = dbObject.dbQuery("SELECT DISTINCT(PriceDate), 1 FROM tblStockPriceData WHERE PriceDate >= '" + str(startDate)+
                                       "' AND PriceDate <= '"+str(endDate)+"'")
        DictOfDates={}
        date_count_range=0
        for date,dummy in resultDates:
            DictOfDates[date]=date_count_range
            date_count_range=date_count_range+1
        query = "SELECT * FROM portfolio_tradesheet_data_table WHERE EntryDate >= '" + str(startDate) + "' AND EntryDate <= '" \
               + str(endDate) + "' AND MetaIndividualId IN " + str(portfolioList)
        resultTrades = dbObject.dbQuery(query)
        c=0
        #Parameters for each individual
        Performance_Measures=[] #List of Tuples to store the Performance_Measures the calculation_type
        #Lists to store the variables for daily calculations
        tmp_daily_running_pl=[]
        #Variables for the total period of between begin date and end date
        total_Win_Long_Trades=0
        total_Win_Short_Trades=0
        total_Loss_Long_Trades=0
        total_Loss_Short_Trades=0
        total_Profit_Long=0
        total_Profit_Short=0
        total_Loss_Long=0
        total_Loss_Short=0

        #Creating a list which stores daily PL values
        for filler_date_count in range(date_count_range):
            tmp_daily_running_pl.append(0)
        date_count=0
        diff=0
        DD_History = [] #List to store the DD values.
        Gain_History = []

        for meta_id, stock_id, individual_id, trade_entry_date, trade_entry_time, trade_entry_price, trade_exit_date, trade_exit_time, trade_exit_price, trade_qty, trade_type in resultTrades:
            individual_id = 0
            CurrentDate=trade_entry_date
            if(c==0):
                prev_trade_entry_date=trade_entry_date
            elif(prev_individual_id==individual_id):
                diff=1
            else:
                if(tmp_daily_running_pl[0] < 0):
                    DD_History.append(tmp_daily_running_pl[0])
                else:
                    DD_History.append(0)
                DD_date_count = 1
                while(DD_date_count < date_count_range):
                    if(tmp_daily_running_pl[DD_date_count]<0):
                        DD_History.append(DD_History[DD_date_count-1]+tmp_daily_running_pl[DD_date_count])
                    else:
                        DD_History.append(0)
                    DD_date_count = DD_date_count +1
                total_DD=0
                for DD_Daily_Value in DD_History:
                    if(DD_Daily_Value<total_DD):
                        total_DD=DD_Daily_Value
                TotalTrades= total_Win_Short_Trades+total_Loss_Short_Trades+total_Win_Long_Trades+total_Loss_Long_Trades
                if(TotalTrades==0 or total_DD==0):
                    Performance_Measures.append((gv.dummyPerformance,gv.dummyPerformance, 0, gv.dummyPerformance, gv.dummyPerformance, 0, 0.0))
                else:
                    if(tmp_daily_running_pl[0]>0):
                        Gain_History.append(tmp_daily_running_pl[0])
                    else:
                        Gain_History.append(0)
                    Gain_date_count=1
                    while(Gain_date_count <date_count_range):
                        if(tmp_daily_running_pl[Gain_date_count]>0):
                            Gain_History.append(Gain_History[Gain_date_count-1]+tmp_daily_running_pl[Gain_date_count])
                        else:
                            Gain_History.append(0)
                        Gain_date_count+=1
                    total_Gain=0.0
                    for Gain_Daily_Value in Gain_History:
                        if(Gain_Daily_Value>total_Gain):
                            total_Gain=Gain_Daily_Value
                    NetPL=total_Profit_Long+total_Loss_Long+total_Profit_Short+total_Loss_Short
                    ProfitMakingEpochs=1.0*(total_Win_Long_Trades+total_Win_Short_Trades)/(1.0*(TotalTrades))
                    Performance_Measures.append((NetPL/(TotalTrades), NetPL/(-total_DD), total_Gain, total_DD, NetPL, TotalTrades, ProfitMakingEpochs))

                Performance_Measures=[] #List of Tuples to store the Performance_Measures the calculation_type
                #Lists to store the variables for daily calculations
                tmp_daily_running_pl=[]
                #Variables for the total period of between begin date and end date
                total_Win_Long_Trades=0
                total_Win_Short_Trades=0
                total_Loss_Long_Trades=0
                total_Loss_Short_Trades=0
                total_Profit_Long=0
                total_Profit_Short=0
                total_Loss_Long=0
                total_Loss_Short=0
                for filler_date_count in range(date_count_range):
                    tmp_daily_running_pl.append(0)

                date_count=0
                diff=0
                DD_History = [] #List to store the DD values.
                Gain_History = [] #List to store the MaxGain values
                IndividualIDExist=0

            if(DictOfDates.has_key(CurrentDate)):
                date_count= DictOfDates[CurrentDate]
                if(trade_type==1):
                    trade_pl = (trade_exit_price - trade_entry_price)*trade_qty
                    if(trade_pl>0):
                        total_Win_Long_Trades+=1
                        total_Profit_Long+=trade_pl
                    else:
                        total_Loss_Long_Trades+=1
                        total_Loss_Long+=trade_pl
                else:
                    trade_pl = (trade_entry_price - trade_exit_price)*trade_qty
                    if(trade_pl>0):
                        total_Win_Short_Trades+=1
                        total_Profit_Short+=trade_pl
                    else:
                        total_Loss_Short_Trades+=1
                        total_Loss_Short+=trade_pl

                tmp_daily_running_pl[date_count] = tmp_daily_running_pl[date_count]+trade_pl
            c=1
            prev_individual_id=individual_id
            prev_trade_entry_date=trade_entry_date
        if(tmp_daily_running_pl[0] < 0):
            DD_History.append(tmp_daily_running_pl[0])
        else:
            DD_History.append(0)
        DD_date_count = 1
        while(DD_date_count < date_count_range):
            if(tmp_daily_running_pl[DD_date_count]<0):
                DD_History.append(DD_History[DD_date_count-1]+tmp_daily_running_pl[DD_date_count])
            else:
                DD_History.append(0)
            DD_date_count = DD_date_count +1
        total_DD=0
        for DD_Daily_Value in DD_History:
            if(DD_Daily_Value<total_DD):
                total_DD=DD_Daily_Value

        TotalTrades= total_Win_Short_Trades+total_Loss_Short_Trades+total_Win_Long_Trades+total_Loss_Long_Trades

        if(TotalTrades==0 or total_DD==0):
            Performance_Measures.append((gv.dummyPerformance,gv.dummyPerformance, 0, gv.dummyPerformance, gv.dummyPerformance, 0, 0.0))
        else:
            #calculation of MaxGain (Similar to DD Calculation):
            if(tmp_daily_running_pl[0]>0):
                Gain_History.append(tmp_daily_running_pl[0])
            else:
                Gain_History.append(0)
            Gain_date_count=1
            while(Gain_date_count <date_count_range):
                if(tmp_daily_running_pl[Gain_date_count]>0):
                    Gain_History.append(Gain_History[Gain_date_count-1]+tmp_daily_running_pl[Gain_date_count])
                else:
                    Gain_History.append(0)
                Gain_date_count+=1
            total_Gain=0.0
            for Gain_Daily_Value in Gain_History:
                if(Gain_Daily_Value>total_Gain):
                    total_Gain=Gain_Daily_Value

            NetPL=total_Profit_Long+total_Loss_Long+total_Profit_Short+total_Loss_Short
            ProfitMakingEpochs=1.0*(total_Win_Long_Trades+total_Win_Short_Trades)/(1.0*(TotalTrades))
            Performance_Measures.append((NetPL/(TotalTrades), NetPL/(-total_DD), total_Gain, total_DD, NetPL, TotalTrades, ProfitMakingEpochs))
        return Performance_Measures

    def calculateReferencePerformanceTradesheet(self, startDate, endDate, dbObject):
        resultDates = dbObject.dbQuery("SELECT DISTINCT(PriceDate), 1 FROM tblStockPriceData WHERE PriceDate >= '" + str(startDate)+
                                       "' AND PriceDate <= '"+str(endDate)+"'")
        DictOfDates={}
        date_count_range=0
        for date,dummy in resultDates:
            DictOfDates[date]=date_count_range
            date_count_range=date_count_range+1
        queryELites = "SELECT IndividualId, SecId FROM tblIndividualCategoryInfo WHERE WalkForwardID=" + str(gv.walkforward) + " AND Category=1"
        resultElites = dbObject.dbQuery(queryELites)
        query = "SELECT * FROM tblIndividualTradesheet WHERE EntryDate >= '" + str(startDate) + "' AND EntryDate <= '" + str(endDate) + "' AND ("
        count = 0
        for feederId, stockId in resultElites:
            if count == 0:
                query += " ( IndividualId=" + str(feederId) + " AND SecId=" + str(stockId) + " )"
                count += 1
            else:
                query += " OR ( IndividualId=" + str(feederId) + " AND SecId=" + str(stockId) + " )"
        query += " )"
        resultTrades = dbObject.dbQuery(query)
        c=0
        #Parameters for each individual
        Performance_Measures=[] #List of Tuples to store the Performance_Measures the calculation_type
        #Lists to store the variables for daily calculations
        tmp_daily_running_pl=[]
        #Variables for the total period of between begin date and end date
        total_Win_Long_Trades=0
        total_Win_Short_Trades=0
        total_Loss_Long_Trades=0
        total_Loss_Short_Trades=0
        total_Profit_Long=0
        total_Profit_Short=0
        total_Loss_Long=0
        total_Loss_Short=0

        #Creating a list which stores daily PL values
        for filler_date_count in range(date_count_range):
            tmp_daily_running_pl.append(0)
        date_count=0
        diff=0
        DD_History = [] #List to store the DD values.
        Gain_History = []

        for stock_id, individual_id, trade_entry_date, trade_entry_time, trade_entry_price, trade_exit_date, trade_exit_time, trade_exit_price, trade_qty, trade_type in resultTrades:
            individual_id = 0
            CurrentDate=trade_entry_date
            if(c==0):
                prev_trade_entry_date=trade_entry_date
            elif(prev_individual_id==individual_id):
                diff=1
            else:
                if(tmp_daily_running_pl[0] < 0):
                    DD_History.append(tmp_daily_running_pl[0])
                else:
                    DD_History.append(0)
                DD_date_count = 1
                while(DD_date_count < date_count_range):
                    if(tmp_daily_running_pl[DD_date_count]<0):
                        DD_History.append(DD_History[DD_date_count-1]+tmp_daily_running_pl[DD_date_count])
                    else:
                        DD_History.append(0)
                    DD_date_count = DD_date_count +1
                total_DD=0
                for DD_Daily_Value in DD_History:
                    if(DD_Daily_Value<total_DD):
                        total_DD=DD_Daily_Value
                TotalTrades= total_Win_Short_Trades+total_Loss_Short_Trades+total_Win_Long_Trades+total_Loss_Long_Trades
                if(TotalTrades==0 or total_DD==0):
                    Performance_Measures.append((gv.dummyPerformance,gv.dummyPerformance, 0, gv.dummyPerformance, gv.dummyPerformance, 0, 0.0))
                else:
                    if(tmp_daily_running_pl[0]>0):
                        Gain_History.append(tmp_daily_running_pl[0])
                    else:
                        Gain_History.append(0)
                    Gain_date_count=1
                    while(Gain_date_count <date_count_range):
                        if(tmp_daily_running_pl[Gain_date_count]>0):
                            Gain_History.append(Gain_History[Gain_date_count-1]+tmp_daily_running_pl[Gain_date_count])
                        else:
                            Gain_History.append(0)
                        Gain_date_count+=1
                    total_Gain=0.0
                    for Gain_Daily_Value in Gain_History:
                        if(Gain_Daily_Value>total_Gain):
                            total_Gain=Gain_Daily_Value
                    NetPL=total_Profit_Long+total_Loss_Long+total_Profit_Short+total_Loss_Short
                    ProfitMakingEpochs=1.0*(total_Win_Long_Trades+total_Win_Short_Trades)/(1.0*(TotalTrades))
                    Performance_Measures.append((NetPL/(TotalTrades), NetPL/(-total_DD), total_Gain, total_DD, NetPL, TotalTrades, ProfitMakingEpochs))

                Performance_Measures=[] #List of Tuples to store the Performance_Measures the calculation_type
                #Lists to store the variables for daily calculations
                tmp_daily_running_pl=[]
                #Variables for the total period of between begin date and end date
                total_Win_Long_Trades=0
                total_Win_Short_Trades=0
                total_Loss_Long_Trades=0
                total_Loss_Short_Trades=0
                total_Profit_Long=0
                total_Profit_Short=0
                total_Loss_Long=0
                total_Loss_Short=0
                for filler_date_count in range(date_count_range):
                    tmp_daily_running_pl.append(0)

                date_count=0
                diff=0
                DD_History = [] #List to store the DD values.
                Gain_History = [] #List to store the MaxGain values
                IndividualIDExist=0

            if(DictOfDates.has_key(CurrentDate)):
                date_count= DictOfDates[CurrentDate]
                if(trade_type==1):
                    trade_pl = (trade_exit_price - trade_entry_price)*trade_qty
                    if(trade_pl>0):
                        total_Win_Long_Trades+=1
                        total_Profit_Long+=trade_pl
                    else:
                        total_Loss_Long_Trades+=1
                        total_Loss_Long+=trade_pl
                else:
                    trade_pl = (trade_entry_price - trade_exit_price)*trade_qty
                    if(trade_pl>0):
                        total_Win_Short_Trades+=1
                        total_Profit_Short+=trade_pl
                    else:
                        total_Loss_Short_Trades+=1
                        total_Loss_Short+=trade_pl

                tmp_daily_running_pl[date_count] = tmp_daily_running_pl[date_count]+trade_pl
            c=1
            prev_individual_id=individual_id
            prev_trade_entry_date=trade_entry_date
        if(tmp_daily_running_pl[0] < 0):
            DD_History.append(tmp_daily_running_pl[0])
        else:
            DD_History.append(0)
        DD_date_count = 1
        while(DD_date_count < date_count_range):
            if(tmp_daily_running_pl[DD_date_count]<0):
                DD_History.append(DD_History[DD_date_count-1]+tmp_daily_running_pl[DD_date_count])
            else:
                DD_History.append(0)
            DD_date_count = DD_date_count +1
        total_DD=0
        for DD_Daily_Value in DD_History:
            if(DD_Daily_Value<total_DD):
                total_DD=DD_Daily_Value

        TotalTrades= total_Win_Short_Trades+total_Loss_Short_Trades+total_Win_Long_Trades+total_Loss_Long_Trades

        if(TotalTrades==0 or total_DD==0):
            Performance_Measures.append((gv.dummyPerformance,gv.dummyPerformance, 0, gv.dummyPerformance, gv.dummyPerformance, 0, 0.0))
        else:
            #calculation of MaxGain (Similar to DD Calculation):
            if(tmp_daily_running_pl[0]>0):
                Gain_History.append(tmp_daily_running_pl[0])
            else:
                Gain_History.append(0)
            Gain_date_count=1
            while(Gain_date_count <date_count_range):
                if(tmp_daily_running_pl[Gain_date_count]>0):
                    Gain_History.append(Gain_History[Gain_date_count-1]+tmp_daily_running_pl[Gain_date_count])
                else:
                    Gain_History.append(0)
                Gain_date_count+=1
            total_Gain=0.0
            for Gain_Daily_Value in Gain_History:
                if(Gain_Daily_Value>total_Gain):
                    total_Gain=Gain_Daily_Value

            NetPL=total_Profit_Long+total_Loss_Long+total_Profit_Short+total_Loss_Short
            ProfitMakingEpochs=1.0*(total_Win_Long_Trades+total_Win_Short_Trades)/(1.0*(TotalTrades))
            Performance_Measures.append((NetPL/(TotalTrades), NetPL/(-total_DD), total_Gain, total_DD, NetPL, TotalTrades, ProfitMakingEpochs))
        return Performance_Measures

if __name__ == "__main__":
    performanceObject = Performance()
    dbObject = DBUtils()
    dbObject.dbConnect()
    performance = performanceObject.calculateReferencePerformanceTradesheet(gv.startDate, gv.endDate, dbObject)
    print(performance)
    dbObject.dbClose()