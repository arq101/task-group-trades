#!/usr/bin/env python

import logging
from xml.dom import minidom
from xml.parsers import expat
import pandas as pd
import sys
import os


LOGFILE = 'server.log'
CSV_OUTPUT = 'results.csv'


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s : %(levelname)s : %(message)s',
    filename=LOGFILE,
    filemode='a'
)
logger = logging.getLogger(__name__)


def read_xml_input_file():
    file_path = _check_cmdline_args()
    try:
        # in order to parse, need to get the entire doc in memory
        logger.info('Parsing input xml file ...')
        xml_doc = minidom.parse(file_path)
    except expat.ExpatError:
        logger.error('Failed to parse XML file! Check file is a valid XML file.')
        raise ValueError
    return xml_doc


def parse_trades_from_xml_data(xml_doc):
    """

    :param xml_doc:
    :return: NodeList of DOM Elements
    """
    trades = xml_doc.getElementsByTagName('Trade')
    logger.info('Trades parsed from input source')
    return trades


def get_dataframe_of_all_input_trades(dom_trades):
    # prepare our empty dataframe object
    df_columns = ['CorrelationID', 'NumberOfTrades', 'Limit', 'Value', 'TradeID']
    df_trades = pd.DataFrame(columns=df_columns)
    logger.info('Generating dataframe object of all trades ...')
    for trade_element in dom_trades:
        correlation_id = trade_element.getAttribute('CorrelationId')
        num_of_trades = trade_element.getAttribute('NumberOfTrades')
        limit = trade_element.getAttribute('Limit')
        trade_ID = trade_element.getAttribute('TradeID')
        value = trade_element.firstChild.data

        series = pd.Series([correlation_id, num_of_trades, limit, value, trade_ID], index=df_columns)
        df_trades = df_trades.append(series, ignore_index=True)

    return df_trades


def get_dataframe_of_aggregated_trades(df_trades_all):
    # store aggregated trades in a new dataframe object
    df_columns = ['CorrelationID', 'NumberOfTrades', 'Limit', 'Value', 'State']
    df_trades_aggr = pd.DataFrame(columns=df_columns)

    df_trades_all[['NumberOfTrades', 'Limit', 'Value']] = df_trades_all[
        ['NumberOfTrades', 'Limit', 'Value']].apply(pd.to_numeric)

    correlation_IDs = df_trades_all['CorrelationID'].unique()
    for corr_ID in correlation_IDs:
        logger.info('Finding and aggregating trades for correlation ID: {}'.format(corr_ID))
        df_tmp = df_trades_all[df_trades_all['CorrelationID'] == corr_ID]
        num_of_trades = df_tmp.iloc[0]['NumberOfTrades']    # expected number of trades
        limit = df_tmp.iloc[0]['Limit']
        aggregated_trade_value = df_tmp['Value'].sum()

        if len(df_tmp) == num_of_trades and aggregated_trade_value <= limit:
            state = 'Accepted'
            logger.info('Trades accepted for correlation ID: {}'.format(corr_ID))
        elif len(df_tmp) < num_of_trades and aggregated_trade_value <= limit:
            state = 'Pending'
            logger.info('Trades pending for correlation ID: {}'.format(corr_ID))
        else:
            state = 'Rejected'
            logger.info('Trades rejected for correlation ID: {}'.format(corr_ID))

        series = pd.Series([corr_ID, num_of_trades, limit, aggregated_trade_value, state],
                           index=df_columns)
        df_trades_aggr = df_trades_aggr.append(series, ignore_index=True)

    df_trades_aggr.sort_values(by=['CorrelationID'], inplace=True)
    df_trades_aggr.reset_index(drop=True, inplace=True)     # drop the index col produced by reset
    return df_trades_aggr


def output_accepted_rejected_trades_to_csv(dataframe_obj, file_path):
    df_results = dataframe_obj[['CorrelationID', 'NumberOfTrades', 'State']]
    df_results.to_csv(file_path, encoding='utf-8', index=False)
    logger.info('finished writing to csv: {}'.format(file_path))


def _check_cmdline_args():
    if len(sys.argv) != 2:
        logger.error('Invalid number of arguments!')
        logger.info('Usage: python {} <input.xml>'.format(os.path.basename(__file__)))
        raise ValueError
    else:
        return sys.argv[1]


def main():
    try:
        doc = read_xml_input_file()
        trades = parse_trades_from_xml_data(doc)
        df_trades = get_dataframe_of_all_input_trades(trades)
        df_accepted_rejected = get_dataframe_of_aggregated_trades(df_trades)
        output_accepted_rejected_trades_to_csv(df_accepted_rejected, CSV_OUTPUT)
        logger.info('--finished successfully--')

    except ValueError:
        logger.error('--ending unsuccessfully--')
    except Exception as err:
        logger.error('Unknown error ... ')
        logger.exception(err)


if __name__ == '__main__':
    main()
