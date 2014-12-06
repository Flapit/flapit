from blpapi import SessionOptions, Session, Name, Event
import csv
import datetime
from flask import Flask, jsonify, request

BLPAPI_HOST = '10.8.8.1'
BLPAPI_PORT = 8194


class StockMarket(object):
    BLPAPI_SERVICE = '//blp/refdata'
    BLPAPI_REQUEST = 'IntradayBarRequest'
    BLPAPI_EVENTY_TYPE = 'TRADE'
    BLPAPI_INTERVAL = 60*24  # In minutes
    BLPAPI_DATA = Name('barData')
    BLPAPI_INTERVAL_DATA = Name('barTickData')
    BLPAPI_FIELD_DATA = Name('close')

    def get_session(self):
        options = SessionOptions()
        options.setServerHost(BLPAPI_HOST)
        options.setServerPort(BLPAPI_PORT)
        return Session(options)

    def get_service(self, session):
        if not session.start():
            raise IOError('Unable to establish session')
        if not session.openService(self.BLPAPI_SERVICE):
            raise IOError('Unable to open service')
        return session.getService(self.BLPAPI_SERVICE)

    def extract_data(self, session):
        while True:
            event = session.nextEvent(1)
            for message in event:
                if message.hasElement(self.BLPAPI_DATA):
                    data = message.getElement(self.BLPAPI_DATA)
                    if data.hasElement(self.BLPAPI_INTERVAL_DATA):
                        data = data.getElement(self.BLPAPI_INTERVAL_DATA)
                        for day in data.values():
                            yield {
                                'open': day.getElement('open').getValueAsFloat(),
                                'close': day.getElement('close').getValueAsFloat(),
                                'high': day.getElement('high').getValueAsFloat(),
                                'low': day.getElement('low').getValueAsFloat(),
                                'time': day.getElement('time').getValueAsDatetime(),
                            }
            if event.eventType() == Event.RESPONSE:
                raise StopIteration()

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
        return list(self.extract_data(session))


app = Flask(__name__)


@app.route('/')
def stock_list():
    return jsonify(app.config['stocks'])


@app.route('/historical', methods=['POST'])
def stock_historical():
    stock = request.form['stock']
    stock_ref = app.config['stocks'][stock]
    stock_market = StockMarket()
    historical = stock_market.request(stock_ref)
    historical = {day.pop('time'): day for day in historical}
    return jsonify(**historical)


if __name__ == '__main__':
    stocks = {}
    with open('FTSE_100.csv', 'rb') as csvfile:
        stock_data = csv.reader(csvfile)
        for key, name in stock_data:
            name = name.replace('"', '').replace('\xa0', '').replace('\xc2', '').strip()
            key = key.replace('"', '').strip()
            stocks[name] = key

    app.config['stocks'] = stocks
    app.debug = True
    app.run(host='0.0.0.0')
