FROM python:3.10

WORKDIR /home/app

COPY requirements.txt /dependencies/requirements.txt
RUN pip install -r /dependencies/requirements.txt

COPY . /home/app
ENV PORT=80
CMD streamlit run --server.port $PORT streamlit_app.py

# $docker build . -t lime   
# $docker run -it -p 80:80 lime
# $docker run -it -v $PWD:/home/app -p 80:80 lime 
