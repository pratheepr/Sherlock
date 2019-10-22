import psycopg2
import sys, os
import numpy as np
import pandas as pds
from sqlalchemy import create_engine
import db_connect as creds
import io

class create_dbConnection:
    def __init__(self, PGUSER, PGPASSWORD, PGHOST, PGDATABASE):
        self.alchemyEngine = create_engine(
            'postgresql+psycopg2://' + PGUSER + ':' + PGPASSWORD + ':@' + PGHOST + '/' + PGDATABASE, pool_recycle=3600)

    def connect_to_db(self) -> object:
        self.dbConnection = self.alchemyEngine.connect()
        return self.dbConnection

class create_DF:
    def __init__(self):
        self.temp = ''

    def createDF(self, dbConnection, sql) -> object:
        self.dataFrame = pds.read_sql(sql, dbConnection)
        return self.dataFrame


def Write_DF_to_DB(postgre_def, df_capital_gains_losses):
    df_capital_gains_losses.head(0).to_sql('public.stg_capt_gains_losses', postgre_def.alchemyEngine, if_exists='replace', index=False)  # truncates the table

    conn = postgre_def.alchemyEngine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df_capital_gains_losses.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, 'public.stg_capt_gains_losses', null="")  # null values become ''
    conn.commit()


def main():
    pds.set_option('display.expand_frame_repr', False)

# Postgre connection definitions
    postgre_def = create_dbConnection(creds.PGUSER, creds.PGPASSWORD, creds.PGHOST, creds.PGDATABASE)
    postgre_conn = postgre_def.connect_to_db()


# Initialize all required dataframes from postrge tables
    df_init = create_DF()

    df_transactions = df_init.createDF(postgre_conn, "select * from public.inp_transaction")
    print(df_transactions)

    df_trans_sell = df_transactions.loc[df_transactions['annotation_value'] == 'Sell']

    df_trans_purchase = df_transactions.loc[df_transactions['annotation_value'] == 'Purchase']

    df_capital_gains_losses = df_init.createDF(postgre_conn, "select * from public.stg_capt_gains_losses where 0=1")

    row_cap_gain_loss = df_init.createDF(postgre_conn, "select * from public.stg_capt_gains_losses where 0=1")

    # Average purchase price calculation  ( avg is multiply price with no_of_items and divide by no. of items )
    purchase_avg_price = (df_trans_purchase.price * df_trans_purchase.no_of_items).sum() / df_trans_purchase.no_of_items.sum()

    sell_count = 0
    purchase_count = 0


    for sell_index, sell_row in df_trans_sell.iterrows():

        sell_count = sell_row['no_of_items']
        purchase_count = 0

        for purch_index, purchase_row in df_trans_purchase.iterrows():

            row_cap_gain_loss = df_init.createDF(postgre_conn,
                                                 "select * from public.stg_capt_gains_losses where 0=1")

            # if sell instrument is greater than purchase
            if ((sell_count - purchase_row['no_of_items']) >= 0) and (purchase_row['no_of_items'] > 0):

                purchase_count = purchase_row['no_of_items'] + purchase_count

                row_cap_gain_loss.at[0, 'transaction'] = 'Sell'
                row_cap_gain_loss.at[0, 'charge_type'] = 'Taxable'
                row_cap_gain_loss.at[0, 'date_trade'] = sell_row['date_trade']
                row_cap_gain_loss.at[0, 'value_date'] = sell_row['date_value']
                row_cap_gain_loss.at[0, 'original_currency'] = 'EU'
                row_cap_gain_loss.at[0, 'exchange_rate'] = 1
                row_cap_gain_loss.at[0, 'pieces'] = purchase_row['no_of_items']
                row_cap_gain_loss.at[0, 'sell_price'] = sell_row['price']

                if creds.CALC_METHOD == 'AVG':
                    row_cap_gain_loss.at[0, 'purchase_price'] = purchase_avg_price
                else:
                    row_cap_gain_loss.at[0, 'purchase_price'] = purchase_row['price']

                row_cap_gain_loss.at[0, 'proceeds_in_euro'] = purchase_row['no_of_items'] * sell_row['price']
                row_cap_gain_loss.at[0, 'fees_charges_in_euro'] = purchase_row['no_of_items'] * sell_row[
                    'fees_per_item']
                row_cap_gain_loss.at[0, 'acquisition_costs_in_euro'] = purchase_row['no_of_items'] * (
                        row_cap_gain_loss.at[0, 'purchase_price'] + purchase_row['fees_per_item'])
                row_cap_gain_loss.at[0, 'capital_gain_loss_in_euro'] = (row_cap_gain_loss.loc[0, 'proceeds_in_euro']) - (row_cap_gain_loss.loc[0, 'acquisition_costs_in_euro'] + row_cap_gain_loss.loc[0, 'fees_charges_in_euro'])
                row_cap_gain_loss.at[0, 'fx_component_in_gain_loss_euro'] = 0
                row_cap_gain_loss.at[0, 'total_gain_loss_eurp'] = row_cap_gain_loss.loc[0, 'capital_gain_loss_in_euro']
                row_cap_gain_loss.at[0, 'sell_transaction_id'] = sell_row['transaction_id']
                row_cap_gain_loss.at[0, 'purchase_transaction_id'] = purchase_row['transaction_id']

                print(row_cap_gain_loss)

                sell_count = sell_count - purchase_row['no_of_items']
                orig_pieces = purchase_row['no_of_items']
                row_cap_gain_loss.at[0, 'orig_pieces'] = orig_pieces

                final_pieces = 0
                df_trans_purchase.at[purch_index, 'no_of_items'] = 0
                row_cap_gain_loss.at[0, 'final_pieces'] = final_pieces

                df_capital_gains_losses = df_capital_gains_losses.append(row_cap_gain_loss, ignore_index= True)

                continue

            if ((sell_count - purchase_row['no_of_items']) < 0)  and (purchase_row['no_of_items'] > 0):

                purchase_count =  sell_count
                row_cap_gain_loss.at[0, 'transaction'] = 'Sell'
                row_cap_gain_loss.at[0, 'charge_type'] = 'Taxable'
                row_cap_gain_loss.at[0, 'date_trade'] = sell_row['date_trade']
                row_cap_gain_loss.at[0, 'value_date'] = sell_row['date_value']
                row_cap_gain_loss.at[0, 'original_currency'] = 'EU'
                row_cap_gain_loss.at[0, 'exchange_rate'] = 1
                row_cap_gain_loss.at[0, 'pieces'] = sell_count
                row_cap_gain_loss.at[0, 'sell_price'] = sell_row['price']

                if creds.CALC_METHOD == 'AVG':
                    row_cap_gain_loss.at[0, 'purchase_price'] = purchase_avg_price
                else:
                    row_cap_gain_loss.at[0, 'purchase_price'] = purchase_row['price']

                row_cap_gain_loss.at[0, 'proceeds_in_euro'] = sell_count * sell_row['price']
                row_cap_gain_loss.at[0, 'fees_charges_in_euro'] = sell_count * sell_row['fees_per_item']
                row_cap_gain_loss.at[0, 'acquisition_costs_in_euro'] = sell_count * (
                        row_cap_gain_loss.at[0, 'purchase_price'] + purchase_row['fees_per_item'])
                row_cap_gain_loss.at[0, 'capital_gain_loss_in_euro'] = row_cap_gain_loss.loc[0, 'proceeds_in_euro'] - \
                                                                       row_cap_gain_loss.loc[0, 'acquisition_costs_in_euro']
                row_cap_gain_loss.at[0, 'fx_component_in_gain_loss_euro'] = 0
                row_cap_gain_loss.at[0, 'total_gain_loss_eurp'] = row_cap_gain_loss.loc[0, 'capital_gain_loss_in_euro']

                df_trans_purchase.at[purch_index, 'no_of_items'] = purchase_row['no_of_items'] - sell_count

                orig_pieces = purchase_row['no_of_items']
                final_pieces = purchase_row['no_of_items'] - sell_count

                row_cap_gain_loss.at[0, 'orig_pieces'] = orig_pieces
                row_cap_gain_loss.at[0, 'final_pieces'] = final_pieces

                row_cap_gain_loss.at[0, 'sell_transaction_id'] = sell_row['transaction_id']
                row_cap_gain_loss.at[0, 'purchase_transaction_id'] = purchase_row['transaction_id']

                sell_count = 0

                df_capital_gains_losses = df_capital_gains_losses.append(row_cap_gain_loss, ignore_index= True)

                continue

            if sell_count == 0:
                break

    Write_DF_to_DB(postgre_def, df_capital_gains_losses)

main()
