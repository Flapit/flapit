from blpapi import SessionOptions, Session, Name, Event
import csv
import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS


class StockMarket(object):
    BLPAPI_SERVICE = '//blp/refdata'
    BLPAPI_REQUEST = 'IntradayBarRequest'
    BLPAPI_EVENTY_TYPE = 'TRADE'
    BLPAPI_INTERVAL = 60*24  # In minutes
    BLPAPI_DATA = Name('barData')
    BLPAPI_INTERVAL_DATA = Name('barTickData')
    BLPAPI_FIELD_DATA = Name('close')

    def __init__(self, host, port):
        self.host = host
        self.port = port

    def get_session(self):
        options = SessionOptions()
        options.setServerHost(self.host)
        options.setServerPort(self.port)
        return Session(options)

    def get_service(self, session):
        if not session.start():
            raise IOError('Unable to establish session')
        if not session.openService(self.BLPAPI_SERVICE):
            raise IOError('Unable to open service')
        return session.getService(self.BLPAPI_SERVICE)

    def get_messages(self, session):
        while True:
            event = session.nextEvent(1)
            for message in event:
                if message.hasElement(self.BLPAPI_DATA):
                    data = message.getElement(self.BLPAPI_DATA)
                    if data.hasElement(self.BLPAPI_INTERVAL_DATA):
                        yield data.getElement(self.BLPAPI_INTERVAL_DATA)
            if event.eventType() == Event.RESPONSE:
                raise StopIteration()

    def get_data_points(self, response):
        for message in response:
            for day in message.values():
                yield {
                    'volume': day.getElement('volume').getValueAsInteger(),
                    'open': day.getElement('open').getValueAsFloat(),
                    'close': day.getElement('close').getValueAsFloat(),
                    'high': day.getElement('high').getValueAsFloat(),
                    'low': day.getElement('low').getValueAsFloat(),
                    'time': day.getElement('time').getValueAsString(),
                }

    def request(self, stock_ref):
        session = self.get_session()
        service = self.get_service(session)
        request = service.createRequest(self.BLPAPI_REQUEST)
        request.set('security', unicode(stock_ref.encode('utf-8')))
        request.set('eventType', self.BLPAPI_EVENTY_TYPE)
        request.set('interval', self.BLPAPI_INTERVAL)
        request.set('startDateTime', datetime.datetime(2014, 1, 1))
        request.set('endDateTime', datetime.datetime.now())
        session.sendRequest(request)
        return self.get_messages(session)

    def get_historical(self, stock_ref):
        response = self.request(stock_ref)
        return list(self.get_data_points(response))


app = Flask(__name__)
CORS(app)


@app.route('/')
def stock_list():
    return jsonify(app.config['stocks'])


@app.route('/historical', methods=['POST'])
def stock_historical():
    stock = request.form['stock']
    stock_ref = app.config['stocks'][stock]
    stock_market = StockMarket('10.8.8.1', 8194)
    historical = stock_market.get_historical(stock_ref)
    return jsonify(historical=historical)


if __name__ == '__main__':
    with open('FTSE_100.csv', 'rb') as csvfile:
        stock_data = csv.reader(csvfile)
        stocks = dict(stock_data)

    app.config['stocks'] = stocks
    app.config.from_object('settings')
    app.debug = True
    app.run()
