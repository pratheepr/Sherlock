import psycopg2
import sys, os
import numpy as np
import pandas as pds
from sqlalchemy import create_engine
import db_connect as creds


class create_dbConnection:
    def __init__(self, PGUSER, PGPASSWORD, PGHOST, PGDATABASE):
        self.alchemyEngine = create_engine(
            'postgresql+psycopg2://' + PGUSER + ':' + PGPASSWORD + ':@' + PGHOST + '/' + PGDATABASE, pool_recycle=3600)
        self.dbConnection = self.alchemyEngine.connect()


class create_DF:
    def __init__(self):
        self.temp = ''

    def createDF(self, dbConnection, sql):
        self.dataFrame = pds.read_sql(sql, dbConnection)
        return self.dataFrame


def main():
    pds.set_option('display.expand_frame_repr', False)

    postgre_conn = create_dbConnection(creds.PGUSER, creds.PGPASSWORD, creds.PGHOST, creds.PGDATABASE)

    df_init = create_DF()

    df_transactions = df_init.createDF(postgre_conn.dbConnection, "select * from public.inp_transaction")

   # print(df_transactions)

    df_trans_sell = df_transactions.loc[df_transactions['annotation_value'] == 'Sell']

    df_trans_purchase = df_transactions.loc[df_transactions['annotation_value'] == 'Purchase']

    df_capital_gains_losses = pds.DataFrame({'transaction': [''], 'charge_type': [''], 'date_trade': [''],
                                                   'date_value': [''],
                                                   'original_currency': [''],  'exchange_rate': [''] ,'pieces': [''],
                                                   'sell_price': [''], 'purchase_price': [''] ,'proceeds_in_euro': [''],
                                                   'fees_charges_in_euro': [''], 'acquisition_costs_in_euro': [''],
                                                   'capital_gain_loss_in_euro': [''],
                                                   'fx_component_in_gain_loss_euro': [''], 'total_gain_loss_eurp': [''],
                                                   'orig_pieces':[''],  'final_pieces': ['']
                                             })

    row_cap_gain_loss = pds.DataFrame({'transaction': [''], 'charge_type': [''], 'date_trade': [''] ,
                                                   'date_value': [''] ,
                                                   'original_currency': [''] ,  'exchange_rate': [''] ,'pieces': [''] ,
                                                   'sell_price': [''], 'purchase_price': [''] ,'proceeds_in_euro': [''],
                                                   'fees_charges_in_euro': [''], 'acquisition_costs_in_euro': [''],
                                                   'capital_gain_loss_in_euro': [''],
                                                   'fx_component_in_gain_loss_euro': [''], 'total_gain_loss_eurp': [''],
                                                   'orig_pieces':[''],  'final_pieces': ['']

                                       })

   # row_cap_gain_loss = pds.DataFrame()

    sell_count = 0
    purchase_count = 0

    for sell_index, sell_row in df_trans_sell.iterrows():
        sell_count = sell_row['no_of_items']
        purchase_count = 0
        for purch_index, purchase_row in df_trans_purchase.iterrows():
            # if (sell_row['no_of_items'] - ( purchase_row['no_of_items'] + purchase_count)  > 0 ) :
            #     purchase_count = purchase_row['no_of_items'] + purchase_count
            #     df_capital_gains_losses = df_capital_gains_losses.append(df_trans_purchase)
            # else:
            #     purchase_count = purchase_count + (sell_count - purchase_row['no_of_items'])
            #     df_trans_purchase.at[purch_index,'no_of_items'] = ( purchase_count + (sell_count - purchase_row['no_of_items']) )

            # if sell instrument is greater than purchase
            if ((sell_count - purchase_row['no_of_items']) >= 0) and (purchase_row['no_of_items'] > 0):
                row_cap_gain_loss = pds.DataFrame({'transaction': [''], 'charge_type': [''], 'date_trade': [''] ,
                                                   'date_value': [''] ,
                                                   'original_currency': [''] ,  'exchange_rate': [''] ,'pieces': [''] ,
                                                   'sell_price': [''], 'purchase_price': [''] ,'proceeds_in_euro': [''],
                                                   'fees_charges_in_euro': [''], 'acquisition_costs_in_euro': [''],
                                                   'capital_gain_loss_in_euro': [''],
                                                   'fx_component_in_gain_loss_euro': [''], 'total_gain_loss_eurp': [''],
                                                   'orig_pieces':[''],  'final_pieces':['']
                                                   })

                purchase_count = purchase_row['no_of_items'] + purchase_count

                row_cap_gain_loss.at[0, 'transaction'] = 'Sell'
                row_cap_gain_loss.at[0, 'charge_type'] = 'Taxable'
                row_cap_gain_loss.at[0, 'date_trade'] = sell_row['date_trade']
                row_cap_gain_loss.at[0, 'date_value'] = sell_row['date_value']
                row_cap_gain_loss.at[0, 'original_currency'] = 'EU'
                row_cap_gain_loss.at[0, 'exchange_rate'] = 1
                row_cap_gain_loss.at[0, 'pieces'] = purchase_row['no_of_items']
                row_cap_gain_loss.at[0, 'sell_price'] = sell_row['price']
                row_cap_gain_loss.at[0, 'purchase_price'] = purchase_row['price']
                row_cap_gain_loss.at[0, 'proceeds_in_euro'] = purchase_row['no_of_items'] * sell_row['price']
                row_cap_gain_loss.at[0, 'fees_charges_in_euro'] = purchase_row['no_of_items'] * sell_row[
                    'fees_per_item']
                row_cap_gain_loss.at[0, 'acquisition_costs_in_euro'] = purchase_row['no_of_items'] * (
                            purchase_row['price'] + purchase_row['fees_per_item'])
                row_cap_gain_loss.at[0, 'capital_gain_loss_in_euro'] = (row_cap_gain_loss.loc[0, 'proceeds_in_euro']) - (row_cap_gain_loss.loc[0, 'acquisition_costs_in_euro'] + row_cap_gain_loss.loc[0, 'fees_charges_in_euro'])
                row_cap_gain_loss.at[0, 'fx_component_in_gain_loss_euro'] = 0
                row_cap_gain_loss.at[0, 'total_gain_loss_eurp'] = row_cap_gain_loss.loc[0, 'capital_gain_loss_in_euro']

                print(row_cap_gain_loss)

               # df_capital_gains_losses =df_capital_gains_losses.append(row_cap_gain_loss, ignore_index= True)

                sell_count = sell_count - purchase_row['no_of_items']
                orig_pieces =  purchase_row['no_of_items']
                row_cap_gain_loss.at[0, 'orig_pieces'] = orig_pieces

                final_pieces = 0
                df_trans_purchase.at[purch_index, 'no_of_items'] = 0
                row_cap_gain_loss.at[0, 'final_pieces'] = final_pieces

                df_capital_gains_losses =df_capital_gains_losses.append(row_cap_gain_loss, ignore_index= True)


                continue

            if ((sell_count - purchase_row['no_of_items']) < 0)  and (purchase_row['no_of_items'] > 0):
                row_cap_gain_loss = pds.DataFrame({'transaction': [''], 'charge_type': [''], 'date_trade': [''],
                                                   'date_value': [''],
                                                   'original_currency': [''], 'exchange_rate': [''], 'pieces': [''],
                                                   'sell_price': [''], 'purchase_price': [''], 'proceeds_in_euro': [''],
                                                   'fees_charges_in_euro': [''], 'acquisition_costs_in_euro': [''],
                                                   'capital_gain_loss_in_euro': [''],
                                                   'fx_component_in_gain_loss_euro': [''], 'total_gain_loss_eurp': ['']
                                                   })

                purchase_count =  sell_count # purchase_count +
                row_cap_gain_loss.at[0, 'transaction'] = 'Sell'
                row_cap_gain_loss.at[0, 'charge_type'] = 'Taxable'
                row_cap_gain_loss.at[0, 'date_trade'] = sell_row['date_trade']
                row_cap_gain_loss.at[0, 'date_value'] = sell_row['date_value']
                row_cap_gain_loss.at[0, 'original_currency'] = 'EU'
                row_cap_gain_loss.at[0, 'exchange_rate'] = 1
                row_cap_gain_loss.at[0, 'pieces'] = sell_count
                row_cap_gain_loss.at[0, 'sell_price'] = sell_row['price']
                row_cap_gain_loss.at[0, 'purchase_price'] = purchase_row['price']
                row_cap_gain_loss.at[0, 'proceeds_in_euro'] = sell_count * sell_row['price']
                row_cap_gain_loss.at[0, 'fees_charges_in_euro'] = sell_count * sell_row['fees_per_item']
                row_cap_gain_loss.at[0, 'acquisition_costs_in_euro'] = sell_count * (
                            purchase_row['price'] + purchase_row['fees_per_item'])
                row_cap_gain_loss.at[0, 'capital_gain_loss_in_euro'] = row_cap_gain_loss.loc[0, 'proceeds_in_euro'] - \
                                                                       row_cap_gain_loss.loc[0, 'acquisition_costs_in_euro']
                row_cap_gain_loss.at[0, 'fx_component_in_gain_loss_euro'] = 0
                row_cap_gain_loss.at[0, 'total_gain_loss_eurp'] = row_cap_gain_loss.loc[0, 'capital_gain_loss_in_euro']


             #   df_capital_gains_losses =df_capital_gains_losses.append(row_cap_gain_loss, ignore_index= True)


                df_trans_purchase.at[purch_index, 'no_of_items'] = purchase_row['no_of_items'] - sell_count

                orig_pieces = purchase_row['no_of_items']
                final_pieces = purchase_row['no_of_items'] - sell_count

                row_cap_gain_loss.at[0, 'orig_pieces'] = orig_pieces
                row_cap_gain_loss.at[0, 'final_pieces'] = final_pieces

                sell_count = 0

                df_capital_gains_losses =df_capital_gains_losses.append(row_cap_gain_loss, ignore_index= True)

                continue

            if sell_count == 0:
                break


            #print(" ***********************PURCHASE TRANSACTIONS*********************** ")
            #print(df_trans_purchase)


    print("PROFIT LOSS STATEMENT  ")
    print(df_capital_gains_losses)


    # print(df_trans_sell)


main()
