# GRUMP : BAW Extraction Utility
A simple utility that uses the BAW REST API's to extract instance data and export it as a CSV.

# Pre-Reqs
Just a recent version of python3, I'm using Python 3.10.

# Install
The easiest way is to download and unpack the zip file.

From the command line where app.py has been extracted just execute the following commands
to install the needed libraries.

`pip3 install -r requirements.txt `

Then run the following command :

`python3 grump.py -c config.yaml`

I do recommend you create a new python virtual environment. If you don't already have virtualenv installed : 

`pip3 install virtualenv`

Then create a virtual environemnt, here mine is called grumpy :

`virtualenv grumpy --python=python3.10`

Then activate the new environment : 

`source grumpy/bin/activate`

There is an example of me doing this in a video here : 
[Grump Video](https://youtu.be/YZXIsKJIy58)