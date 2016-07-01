from flask import Flask, request, abort, jsonify, make_response
import datetime
from mockData.Mock import mockData
from createPackage import *
app = Flask(__name__)

@app.route('/')
def homepage():
    the_time = datetime.datetime.now().strftime("%A, %d %b %Y %l:%M %p")

    return """
    <h1>Hello justgoer</h1>
    <p>It is currently {time}.</p>

    <img src="http://loremflickr.com/600/400" />
    """.format(time=the_time)

@app.route("/hello", methods=["GET"])
def hello():
    return "Hello"

@app.route("/getPackages", methods=["GET"])
def get_packages():
    return jsonify(mockData), 200

@app.route("/isActive", methods=["GET"])
def is_active():
    return "ACTIVE"


@app.route("/getRealPackages", methods=["GET"])
def getRealPackages():
    if not request.args.get('origin'):
        message = "<h1>This API requires parameter for origin</h1>" + \
        "<p>Usage: /getRealPackages?origin=[ORIGIN]</p>"
        abort(make_response(message, 400))
    origin = request.args.get('origin')
    print "get request origin " + origin
    # job = graphlab.deploy.job.create(createOriginframe,origin=origin)
    # return 'Job created'
    result = createOriginframe(origin)
    return result
if __name__ == '__main__':
    app.run(debug=True, use_reloader=True)












