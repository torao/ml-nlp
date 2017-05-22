FROM ubuntu:16.04
MAINTAINER TAKAMI Torao <koiroha@gmail.com>

RUN apt-get update
RUN apt-get install -y g++ make git curl file openssl perl bzip2 xz-utils

# MeCab 0.996
RUN curl -o mecab-0.996.tar.gz -L 'https://drive.google.com/uc?export=download&id=0B4y35FiV1wh7cENtOXlicTFaRUE'
RUN tar zxfv mecab-0.996.tar.gz
RUN cd mecab-0.996; ./configure; make; make check; make install
RUN ldconfig
RUN rm -rf mecab-0.996 mecab-0.996.tar.gz

# MeCab IPADIC
RUN curl -o mecab-ipadic-2.7.0-20070801.tar.gz -L 'https://drive.google.com/uc?export=download&id=0B4y35FiV1wh7MWVlSDBCSXZMTXM'
RUN tar zxfv mecab-ipadic-2.7.0-20070801.tar.gz
RUN cd mecab-ipadic-2.7.0-20070801; ./configure --with-charset=utf8; make; make install
RUN rm -rf mecab-ipadic-2.7.0-20070801 mecab-ipadic-2.7.0-20070801.tar.gz

# NEologd
RUN git clone --depth 1 https://github.com/neologd/mecab-ipadic-neologd.git
RUN cd mecab-ipadic-neologd; ./bin/install-mecab-ipadic-neologd -n -a -y
RUN rm -rf mecab-ipadic-neologd

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

RUN apt-get remove -y g++ make curl git file openssl perl bzip2

ENTRYPOINT ["cabocha", "-d", "/usr/local/lib/mecab/dic/mecab-ipadic-neologd"]