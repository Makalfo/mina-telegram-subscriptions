# import libraries
import os
import sys
import pandas as pd
import pandas.io.sql as sqlio
import json
import configparser
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import warnings
import logging
warnings.filterwarnings("ignore")

# Telegram 
from telegram.ext.updater import Updater
from telegram.update import Update
from telegram.ext.callbackcontext import CallbackContext
from telegram.ext.commandhandler import CommandHandler
from telegram.ext.messagehandler import MessageHandler
from telegram.ext.filters import Filters
from telegram.error import (TelegramError, Unauthorized, BadRequest, 
                            TimedOut, ChatMigrated, NetworkError)

logging.getLogger(__name__).addHandler(logging.StreamHandler(sys.stdout))

class MinaSubscriptions:

    def __init__( self, mode='nominal'):

        # log levels and mode
        self.mode = os.getenv('MODE')
        if self.mode == None:
            self.mode = 'nominal'

        if self.mode == 'nominal' :
            log_level = logging.INFO
        else:
            log_level = logging.DEBUG

        logging.basicConfig( format = '%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
            level = log_level )

        # save the logger
        self.logger = logging
        self.logger.info( 'Starting Up Mina Telegram Subscription Bot' )

        # log the mode
        self.mode = mode
        self.logger.info( f'Operation Mode: {self.mode}' )

        # valid actions
        self.actions = [ 'blocks', 'transactions' ]
        self.valid_subscribe = '\n'.join( [ "To Subscribe to Block Production Alerts, type '/subscribe blocks <Public Key>'",
                                            "To Subscribe to Transaction Alerts, type '/subscribe transactions <Public Key>'"] )
        self.valid_unsubscribe = '\n'.join( [ "To Unsubscribe to Block Production Alerts, type '/unsubscribe blocks <Public Key>'",
                                              "To Unsubscribe to Transaction Alerts, type '/unsubscribe transactions <Public Key>'",
                                              "To Unsubscribe to All Alerts, type '/unsubscribe all'"] )
        # max subs
        self.max_subs = 10

        # connect to database
        self.subscription = self.connect_db( {
            'database':  os.getenv('SUBSCRIPTION_DATABASE'),
            'host': os.getenv('SUBSCRIPTION_HOST'),
            'port': os.getenv('SUBSCRIPTION_PORT'),
            'user': os.getenv('SUBSCRIPTION_USER'),
            'password': os.getenv('SUBSCRIPTION_PASSWORD' ),
        } )
        self.cursor = self.subscription.cursor()

        # connect to telegram
        self.telegram = Updater( os.getenv('TELEGRAM_TOKEN' ),
                    use_context=True)

        self.telegram.dispatcher.add_handler(CommandHandler('start', self.start))
        self.telegram.dispatcher.add_handler(CommandHandler('subscribe', self.subscribe))
        self.telegram.dispatcher.add_handler(CommandHandler('unsubscribe', self.unsubscribe))
        self.telegram.dispatcher.add_handler(CommandHandler('help', self.help))
        self.telegram.dispatcher.add_handler(MessageHandler(Filters.text, self.unknown))
        self.telegram.dispatcher.add_handler(MessageHandler(
            Filters.command, self.unknown)) # Filters out unknown commands
        self.telegram.dispatcher.add_error_handler(self.error_callback)

        # Filters out unknown messages.
        self.telegram.dispatcher.add_handler(MessageHandler(Filters.text, self.unknown_text))

        self.logger.info( f'Start Polling...' )
        self.telegram.start_polling(timeout=600)

    def read_file( self, filename ):
        '''read the file'''
        with open(filename, 'r') as f:
            return f.read().replace("\n","")

    def connect_db( self, info ):
        '''establish the postgres'''
        self.logger.info( f"Connecting to {info[ 'database' ]} at {info[ 'host' ]}:{info[ 'port' ]}")
        # connect
        conn = psycopg2.connect(
            database =  info[ 'database' ],
            user =      info[ 'user' ],
            password =  info[ 'password' ],
            host =      info[ 'host' ],
            port =      info[ 'port' ] )
        # set isolation level
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);
        return conn

    def insert_block_subscription( self, id, name, first, public_key ):
        '''insert the subscription'''
        self.logger.info( f'Inserting {[ id, name, first, public_key ]}' )
        cmd = """INSERT INTO blocks (
            telegram_id,
            telegram_name ,
            telegram_first,
            public_key
            ) VALUES (%s, %s, %s, %s)"""
        self.cursor.execute( cmd, ( id, name, first, public_key ) )

    def check_block_subscription( self, id, name, first, public_key ):
        '''get subscription entries that match'''
        cmd = """SELECT "id" FROM blocks
                WHERE "telegram_id" = '%s'
                AND "telegram_name" = '%s'
                AND "telegram_first" = '%s'
                AND "public_key" = '%s' """ % ( id, name, first, public_key )
        return list( self.get_df_data( cmd )[ 'id' ] )
    
    def get_num_block_subscriptions( self, id, name, first ):
        '''check how many subscriptions exist'''
        cmd = """SELECT "id" FROM blocks
                        WHERE "telegram_id" = '%s'
                        AND "telegram_name" = '%s'
                        AND "telegram_first" = '%s' """ % ( id, name, first )
        return self.get_df_data( cmd )[ 'id' ]

    def delete_block_subscriptions( self, ids: str ):
        '''delete subscriptions'''
        print( ids )
        cmd = """DELETE FROM blocks
                WHERE "id" IN (%s) """ % ids
        self.cursor.execute( cmd )

    def insert_transaction_subscription( self, id, name, first, public_key ):
        '''insert the subscription'''
        self.logger.info( f'Inserting {[ id, name, first, public_key ]}' )
        cmd = """INSERT INTO transactions (
            telegram_id,
            telegram_name ,
            telegram_first,
            public_key
            ) VALUES (%s, %s, %s, %s)"""
        self.cursor.execute( cmd, ( id, name, first, public_key ) )

    def check_transaction_subscription( self, id, name, first, public_key ):
        '''get subscription entries that match'''
        cmd = """SELECT "id" FROM transactions
                WHERE "telegram_id" = '%s'
                AND "telegram_name" = '%s'
                AND "telegram_first" = '%s'
                AND "public_key" = '%s' """ % ( id, name, first, public_key )
        return list( self.get_df_data( cmd )[ 'id' ] )
    
    def get_num_transaction_subscriptions( self, id, name, first ):
        '''check how many subscriptions exist'''
        cmd = """SELECT "id" FROM transactions
                        WHERE "telegram_id" = '%s'
                        AND "telegram_name" = '%s'
                        AND "telegram_first" = '%s' """ % ( id, name, first )
        return self.get_df_data( cmd )[ 'id' ]

    def delete_transaction_subscriptions( self, ids: str ):
        '''delete subscriptions'''
        print( ids )
        cmd = """DELETE FROM transactions
                WHERE "id" IN (%s) """ % ids
        self.cursor.execute( cmd )

    def save_data( self, data, file_name ):
        '''save the data'''
        self.logger.info( f'Saving Data to {file_name}' )
        with open(file_name, "w") as outfile:
            json.dump( data, outfile, indent=4)

    def load_data( self, file_name ):
        '''load the data''' 
        if os.path.exists( file_name ):
            self.logger.info( f'Loading Data from {file_name}' )
            with open(file_name) as json_file:
                return json.load( json_file )
        else:
            return dict()

    def get_df_data( self, query ):
        '''query the database'''
        df = pd.DataFrame()
        for chunk in sqlio.read_sql_query( query, self.subscription, chunksize=10000 ):
            df = pd.concat([ df, chunk ])
        return df

    def start( self, update: Update, context: CallbackContext):
        update.message.reply_text(
            "Welcome to Mina Block Producer and Transaction Alerts!\nType /help for available commands.")

    def help( self, update: Update, context: CallbackContext):
        message = [ "Available Commands:\n",
                    "/subscribe blocks <Public Key>\t- Alerts on Block Production for Public Key",
                    "/subscribe transactions <Public Key>\t- Alerts on Transactions for Public Key",
                    "/unsubscribe blocks <Public Key>\t- Unsubscribe for Block Producer Alerts",
                    "/unsubscribe transactions <Public Key>\t- Unsubscribe for Transaction Alerts",
                    "/unsubscribe all\t\t- Unsubscribe for All Alerts"]
        update.message.reply_text("\n".join( message ))

    def subscribe( self, update: Update, context: CallbackContext):
        # get the public key
        user = update.message.from_user
        self.logger.info( f"Subscribe Request received from: {user} with { context.args }")
        if len( context.args ) == 2:
            action = str( context.args[0] )
            public_key = str( context.args[1] )
            valid_key = self.validate_key( public_key )
            if valid_key[ 'valid' ] and action in self.actions:
                if action == 'blocks':
                    self.subscribe_blocks( update, user, public_key )
                elif action == 'transactions':
                    self.subscribe_transactions( update, user, public_key )
            else:
                if action not in self.actions:
                    self.logger.warning( f"Option Not Found: {action}." )
                    update.message.reply_text( f"To Subscribe, Provide a Valid Option.\nInvalid: {action}\n{self.valid_subscribe}" )
                elif valid_key[ 'error' ] != None:
                    self.logger.warning( f"Error Found: {valid_key[ 'error' ]}." )
                    update.message.reply_text( f"To Subscribe, Provide a Valid Public Key.\nInvalid: {public_key}\n{self.valid_subscribe}" )
        else:
            self.logger.warning( f"Unable to Subscribe {user} - No Option or Public Key Provided")
            update.message.reply_text( f"Unable to Subscribe.\n{self.valid_subscribe}" )

    def subscribe_blocks( self, update: Update, user: dict, public_key: str ):
        '''subscribe for block alerts'''
        # check if already registered
        if len( self.check_block_subscription( user[ 'id' ], user[ 'username' ], user[ 'first_name' ], public_key) ) == 0:
            if len( self.get_num_block_subscriptions( user[ 'id' ], user[ 'username' ], user[ 'first_name' ] ) ) <= int( self.max_subs ):
                # add the block producer to the subscriptions
                self.logger.info( f"Subscribing {user} for blocks { public_key }")                
                self.insert_block_subscription( user[ 'id' ], user[ 'username' ], user[ 'first_name' ], public_key )
                update.message.reply_text( f"Successfully Subscribed to { public_key } for Block Alerts" )
            else:
                # max subscriptions reached
                update.message.reply_text( f"Max Number of Block Producer Subscriptions ( {self.max_subs} ) Reached." )
        else:
            # already subscribed
            self.logger.warning( f"{user} Already Subscribed to { public_key } for Block Production Alerts" )
            update.message.reply_text( f"Already Subscribed to { public_key } for Block Production Alerts" )

    def subscribe_transactions( self, update: Update, user: dict, public_key: str ):
        # check if already registered
        if len( self.check_transaction_subscription( user[ 'id' ], user[ 'username' ], user[ 'first_name' ], public_key) ) == 0:
            if len( self.get_num_transaction_subscriptions( user[ 'id' ], user[ 'username' ], user[ 'first_name' ] ) ) <= int( self.max_subs ):
                # add the block producer to the subscriptions
                self.logger.info( f"Subscribing {user} for transactions { public_key }")                
                self.insert_transaction_subscription( user[ 'id' ], user[ 'username' ], user[ 'first_name' ], public_key )
                update.message.reply_text( f"Successfully Subscribed to { public_key } for Transaction Alerts" )
            else:
                # max subscriptions reached
                update.message.reply_text( f"Max Number of Transaction Subscriptions ( {self.max_subs} ) Reached." )
        else:
            # already subscribed
            self.logger.warning( f"{user} Already Subscribed to { public_key } for Transaction Alerts" )
            update.message.reply_text( f"Already Subscribed to { public_key } for Transaction Alerts" )

    def unsubscribe( self, update: Update, context: CallbackContext):
        # get the public key
        user = update.message.from_user
        self.logger.info( f"Unsubscribe Request received from: {user} with { context.args }")

        # current subscriptions for user
        current_block_subs = self.get_num_block_subscriptions( user[ 'id' ], user[ 'username' ], user[ 'first_name' ] )
        current_transaction_subs = self.get_num_transaction_subscriptions( user[ 'id' ], user[ 'username' ], user[ 'first_name' ] )

        # verify they are subscribed
        if len( current_block_subs ) > 0 or len( current_transaction_subs ) > 0:
            # check if delete all subs
            if len( context.args ) == 1:
                action = str( context.args[0] )
                if action == 'all':
                    self.logger.info( f"Unsubscribing {user} from All Alerts")  
                    if len( current_block_subs ) > 0:
                        self.delete_block_subscriptions( ','.join( str( v ) for v in current_block_subs ) )
                    if len( current_transaction_subs ) > 0:
                        self.delete_transaction_subscriptions( ','.join( str( v ) for v in current_transaction_subs ) )
                    update.message.reply_text( f"Successfully Unsubscribed to All Alerts" )
                else:
                    self.logger.warning( f"Invalid Option {action}. To unsubscribe from all - '/unsubscribe all'" )
                    update.message.reply_text( f"Invalid Option {action}.\n{self.valid_unsubscribe}" )
            elif len( context.args ) == 2:
                action = str( context.args[0] )
                public_key = str( context.args[1] )
                valid_key = self.validate_key( public_key )
                if valid_key[ 'valid' ] and action in self.actions:
                    # remove the block producer from the subscriptions
                    if action == 'blocks':
                        # check if subscribed
                        block_subs = self.check_block_subscription( user[ 'id' ], user[ 'username' ], user[ 'first_name' ], public_key )
                        if len( block_subs ) > 0:
                            self.logger.info( f"Unsubscribing {user} from { public_key }")  
                            self.delete_block_subscriptions( ','.join( str( v ) for v in block_subs ) )
                            update.message.reply_text( f"Successfully Unsubscribed to { public_key } for Block Producer Alerts" )
                        else:
                            self.logger.warning( f"Not Unsubscribed to Block Producer Alerts { public_key } for {user}")
                            update.message.reply_text( f"Unable to Unsubscribe - Not Subscribed Block Producer Alerts for Public Key: {public_key} " )
                    elif action == 'transactions':
                        transaction_subs = self.check_transaction_subscription( user[ 'id' ], user[ 'username' ], user[ 'first_name' ], public_key )
                        if len( transaction_subs ) > 0:
                            self.logger.info( f"Unsubscribing {user} from { public_key }")  
                            self.delete_transaction_subscriptions( ','.join( str( v ) for v in transaction_subs ) )
                            update.message.reply_text( f"Successfully Unsubscribed to { public_key } for Transaction Alerts" )
                        else:
                            self.logger.warning( f"Not Unsubscribed to Transaction Alerts { public_key } for {user}")
                            update.message.reply_text( f"Unable to Unsubscribe - Not Subscribed Transaction Alerts for Public Key: {public_key} " )
                else: 
                    if action not in self.actions:
                        self.logger.warning( f"Option Not Found: {action}." )
                        update.message.reply_text( f"To Unsubscribe, Provide a Valid Option.\nInvalid: {action}\n{self.valid_unsubscribe}" )
                    elif valid_key[ 'error' ] != None:
                        self.logger.warning( f"Error Found: {valid_key[ 'error' ]}." )
                        update.message.reply_text( f"To Unsubscribe, Provide a Valid Public Key.\nInvalid: {public_key}\n{self.valid_unsubscribe}" )
            else:
                self.logger.warning( "To Unsubscribe from All, type '/unsubscribe all'" )
                update.message.reply_text( "To Unsubscribe from All, type '/unsubscribe all'" )
        else:
            self.logger.info( f"Not Subscribed to any alerts for {user}" )  
            update.message.reply_text( f"Not Subscribed to Any Block Producer or Transaction Alerts!" )

    def validate_key( self, public_key ):
        '''validate the key'''
        error = None
        
        if len( public_key ) != 55:
            error = 'Improper Public Key Length'
        if not public_key.isalnum():
            error = 'Improper Characters Found'
        if not public_key.startswith( "B62" ) :
            error = 'Improper Charactes Found'
        if any([x.lower() in public_key.lower() for x in [ 'drop', 'trunc', 'delete', 'insert' ] ] ):
            error = 'Reserved Word Found'
        
        if error != None:
            return {  'error': error,
                       'valid': False }
        else:
            return {  'error': None,
                       'valid': True }

    def unknown( self, update: Update, context: CallbackContext):
        update.message.reply_text(
            "Sorry - '%s' is not a valid command" % update.message.text)

    def unknown_text( self, update: Update, context: CallbackContext):
        update.message.reply_text(
            "Sorry I can't recognize you, you said '%s'" % update.message.text)

    def error_callback(self, update: Update, context: CallbackContext):
        raise context.error
    

subscriptions = MinaSubscriptions( )


