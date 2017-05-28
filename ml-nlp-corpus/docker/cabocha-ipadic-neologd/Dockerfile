# docker build -t torao/cabocha-ipadic-neologd:1.0.1 cabocha-ipadic-neologd
FROM torao/mecab-ipadic-neologd:1.0.1
MAINTAINER TAKAMI Torao <koiroha@gmail.com>

RUN apt-get update
RUN apt-get install -y g++ make curl bzip2

# CRF++ 0.58
RUN curl -o CRF++-0.58.tar.gz -L 'https://drive.google.com/uc?export=download&id=0B4y35FiV1wh7QVR6VXJ5dWExSTQ'
RUN tar zxfv CRF++-0.58.tar.gz
RUN cd CRF++-0.58; ./configure; make; make install
RUN ldconfig
RUN rm -rf CRF++-0.58 CRF++-0.58.tar.gz

# CaboCha 0.69
#RUN curl -o cabocha-0.69.tar.bz2 -L 'https://drive.google.com/uc?export=download&confirm=Tl_g&id=0B4y35FiV1wh7SDd1Q1dUQkZQaUU'
COPY cabocha-0.69.tar.bz2 .
RUN ls -laF
RUN tar jxvf cabocha-0.69.tar.bz2
RUN cat cabocha-0.69/src/chunk_learner.cpp
RUN cd cabocha-0.69; ./configure --with-charset=utf8; make; make install
RUN ldconfig
RUN rm -rf cabocha-0.69 cabocha-0.69.tar.bz2

RUN apt-get remove -y g++ make curl bzip2

ENTRYPOINT ["cabocha", "-d", "/usr/local/lib/mecab/dic/mecab-ipadic-neologd"]