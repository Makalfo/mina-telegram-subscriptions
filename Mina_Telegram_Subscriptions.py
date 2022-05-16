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

logging.getLogger(__name__).addHandler(logging.StreamHandler(sys.stdout))
logging.basicConfig( format = '%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
            level = logging.INFO )

class MinaSubscriptions:

    def __init__( self, mode='nominal'):

        # file names
        self.files = {  'config': 'config.ini' }

        # log levels and mode
        self.mode = mode

        # save the logger
        self.logger = logging
        self.logger.info( 'Starting Up Mina Telegram Subscription Bot' )

        # log the mode
        self.mode = mode
        self.logger.info( f'Operation Mode: {self.mode}' )

        # read config file
        self.config = self.read_config( )

        # connect to database
        self.subscription = self.connect_db( {
            'database':  os.getenv('SUBSCRIPTION_DATABASE'),
            'host': os.getenv('SUBSCRIPTION_HOST'),
            'port': os.getenv('SUBSCRIPTION_PORT'),
            'user': os.getenv('SUBSCRIPTION_USER'),
            'password': os.getenv('SUBSCRIPTION_PASSWORD' ),
        } )
        self.cursor = self.subscription.cursor()

        # create database
        if mode == "setup":
            self.create_subscription_database()
            self.create_subscription_table() 

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

        # Filters out unknown messages.
        self.telegram.dispatcher.add_handler(MessageHandler(Filters.text, self.unknown_text))

        self.logger.info( f'Start Polling...' )
        self.telegram.start_polling()

    def read_file( self, filename ):
        '''read the file'''
        with open(filename, 'r') as f:
            return f.read().replace("\n","")

    def read_config( self ):
        '''read the config file'''
        config = configparser.ConfigParser(interpolation=None)
        config.read( self.files[ 'config' ] )
        return config

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

    def create_subscription_database( self ):
        '''create subscription database'''
        self.cursor.execute( "create database subscriptions" )

    def create_subscription_table( self ):
        '''create table'''
        cmd = """CREATE TABLE subscription (
            id SERIAL PRIMARY KEY,
            telegram_id INTEGER NOT NULL,
            telegram_name VARCHAR(255) NOT NULL,
            telegram_first VARCHAR(255) NOT NULL,
            block_producer VARCHAR(255) NOT NULL
            )"""
        self.cursor.execute( cmd )

    def insert_subscription( self, id, name, first, public_key ):
        '''insert the subscription'''
        self.logger.info( f'Inserting {[ id, name, first, public_key ]}' )
        cmd = """INSERT INTO subscription (
            telegram_id,
            telegram_name ,
            telegram_first,
            block_producer
            ) VALUES (%s, %s, %s, %s)"""
        self.cursor.execute( cmd, ( id, name, first, public_key ) )

    def check_subscription( self, id, name, first, public_key ):
        '''get subscription entries that match'''
        cmd = """SELECT "id" FROM subscription
                WHERE "telegram_id" = '%s'
                AND "telegram_name" = '%s'
                AND "telegram_first" = '%s'
                AND "block_producer" = '%s' """ % ( id, name, first, public_key )
        return list( self.get_df_data( cmd )[ 'id' ] )
    
    def get_num_subscriptions( self, id, name, first ):
        '''check how many subscriptions exist'''
        cmd = """SELECT "id" FROM subscription
                        WHERE "telegram_id" = '%s'
                        AND "telegram_name" = '%s'
                        AND "telegram_first" = '%s' """ % ( id, name, first )
        return self.get_df_data( cmd )[ 'id' ]

    def delete_subscriptions( self, ids: str ):
        '''delete subscriptions'''
        print( ids )
        cmd = """DELETE FROM subscription
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
            "Welcome to Mina Block Producer Alerts!\nType /help for available commands.")

    def help( self, update: Update, context: CallbackContext):
        message = [ "Available Commands:\n",
                    "/subscribe <Block Producer Key>\t- Alerts on Block Production for Public Key",
                    "/unsubscribe <Block Producer Key>\t- Unsubscribe for Block Producer Alerts",
                    "/unsubscribe all\t\t- Unsubscribe for All Alerts"]
        update.message.reply_text("\n".join( message ))

    def subscribe( self, update: Update, context: CallbackContext):
        # get the public key
        user = update.message.from_user
        self.logger.info( f"Subscribe Request received from: {user} with { context.args }")
        if len( context.args ) == 1:
            public_key = str( context.args[0] )
            if len( public_key ) == 55 and public_key.isalnum() and "B62" in public_key:
                # check if already registered
                if len( self.check_subscription( user[ 'id' ], user[ 'username' ], user[ 'first_name' ], public_key) ) == 0:
                    if len( self.get_num_subscriptions( user[ 'id' ], user[ 'username' ], user[ 'first_name' ] ) ) <= int( self.config[ 'Config' ][ 'max_subs' ] ):
                        # add the block producer to the subscriptions
                        self.logger.info( f"Subscribing {user} for { public_key }")                
                        self.insert_subscription( user[ 'id' ], user[ 'username' ], user[ 'first_name' ], public_key )
                        update.message.reply_text( f"Successfully Subscribed to { public_key }" )
                    else:
                        # max subscriptions reached
                        update.message.reply_text( f"Max Number of Subscriptions Reached." )
                else:
                    # already subscribed
                    self.logger.warning( f"{user} Already Subscribed to { public_key }" )
                    update.message.reply_text( f"Already Subscribed to { public_key }" )
            else:
                self.logger.warning( "To Subscribe, Provide a Block Producer Key." )
                update.message.reply_text( f"To Subscribe, Provide a Valid Block Producer Key.\nInvalid: {public_key}\nTo Subscribe, type '/subscribe <Block Producer Key>'" )
        else:
            self.logger.warning( f"Unable to Subscribe {user} - No Block Producer Key Provided")
            update.message.reply_text( f"Unable to Subscribe - No Block Producer Key Provided.\nTo Subscribe, type '/subscribe <Block Producer Key>'" )

    def unsubscribe( self, update: Update, context: CallbackContext):
        # get the public key
        user = update.message.from_user
        self.logger.info( f"Unsubscribe Request received from: {user} with { context.args }")

        # current subscriptions for user
        current_subs = self.get_num_subscriptions( user[ 'id' ], user[ 'username' ], user[ 'first_name' ] )

        # check if delete all subs
        if len( context.args ) == 1:
            public_key = str( context.args[0] )
            if public_key.isalnum():
                if public_key == "all" and len( current_subs ) != 0:
                    self.logger.info( f"Unsubscribing {user} from All Block Producers")  
                    self.delete_subscriptions( ','.join( str( v ) for v in current_subs ) )
                    update.message.reply_text( f"Successfully Unsubscribed to All Block Producers" )
                elif len( public_key ) == 55 and public_key.isalnum() and "B62" in public_key:
                    # check if subscribed
                    key_subs = self.check_subscription( user[ 'id' ], user[ 'username' ], user[ 'first_name' ], public_key )
                    if len( key_subs ) != 0:
                        # add the block producer to the subscriptions
                        self.logger.info( f"Unsubscribing {user} from { public_key }")  
                        self.delete_subscriptions( ','.join( str( v ) for v in key_subs ) )
                        update.message.reply_text( f"Successfully Unsubscribed to { public_key }" )
                    else:
                        # not subscribed
                        self.logger.warning( f"{user} Not Subscribed to { public_key }")
                        update.message.reply_text( f"Not Subscribed to { public_key }" )
                else:
                    self.logger.warning( f"Not Unsubscribed to { public_key } for {user} - Invalid Block Producer Key")
                    update.message.reply_text( f"Unable to Unsubscribe - Invalid Block Producer Key: {public_key}" )
            else:
                self.logger.warning( f"Invalid Block Producer Key {public_key}. To unsubscribe from all - '/unsubscribe all'" )
                update.message.reply_text( f"Invalid Block Producer Key {public_key}.\nTo unsubscribe from all - '/unsubscribe all'" )
        else:
            self.logger.warning( "To Unsubscribe from All, type '/unsubscribe all'" )
            update.message.reply_text( "To Unsubscribe from All, type '/unsubscribe all'" )

    def unknown( self, update: Update, context: CallbackContext):
        update.message.reply_text(
            "Sorry - '%s' is not a valid command" % update.message.text)

    def unknown_text( self, update: Update, context: CallbackContext):
        update.message.reply_text(
            "Sorry I can't recognize you, you said '%s'" % update.message.text)

subscriptions = MinaSubscriptions( )


