FROM java:8-jre-alpine

ENV GOSU_VERSION 1.10
ENV SPARK_VERSION 3.0.0
ENV SPARK_HOME /usr/local/spark
ENV SPARK_LOCAL_DIRS /usr/local/spark/work
ENV SPARK_USER aml
ARG GLIBC_APKVER=2.27-r0
ENV LANG=en_US.UTF-8

LABEL vendor=ActionML \
      version_tags="[\"2.1\",\"2.1.2\"]"

# Update alpine and install required tools
RUN echo "@community http://nl.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories && \
    apk add --update --no-cache bash curl shadow@community

# Glibc compatibility
RUN curl -sSL https://github.com/sgerrand/alpine-pkg-glibc/releases/download/$GLIBC_APKVER/sgerrand.rsa.pub \
            -o /etc/apk/keys/sgerrand.rsa.pub && \
    curl -sSLO https://github.com/sgerrand/alpine-pkg-glibc/releases/download/$GLIBC_APKVER/glibc-i18n-$GLIBC_APKVER.apk && \
    curl -sSLO https://github.com/sgerrand/alpine-pkg-glibc/releases/download/$GLIBC_APKVER/glibc-$GLIBC_APKVER.apk && \
    curl -sSLO https://github.com/sgerrand/alpine-pkg-glibc/releases/download/$GLIBC_APKVER/glibc-bin-$GLIBC_APKVER.apk && \
    apk add --no-cache glibc-$GLIBC_APKVER.apk glibc-bin-$GLIBC_APKVER.apk glibc-i18n-$GLIBC_APKVER.apk && \
    echo "export LANG=$LANG" > /etc/profile.d/locale.sh && \
      rm /etc/apk/keys/sgerrand.rsa.pub glibc-*.apk

# Get gosu
RUN apk add --update --no-cache --virtual .deps gnupg && \
    curl -sSL https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-amd64 \
         -o /usr/local/bin/gosu && chmod 755 /usr/local/bin/gosu \
    && curl -sSL -o /tmp/gosu.asc https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-amd64.asc \
    && export GNUPGHOME=/tmp \
    && gpg --keyserver ha.pool.sks-keyservers.net --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 \
    && gpg --batch --verify /tmp/gosu.asc /usr/local/bin/gosu \
    && rm -r /tmp/* && apk del .deps

# Fetch and unpack spark dist
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz \
      | tar -xzp -C /usr/local/ && \
        ln -s spark-${SPARK_VERSION}-bin-hadoop2.7 ${SPARK_HOME}

# Create users (to go "non-root") and set directory permissions
RUN useradd -mU -d /home/hadoop hadoop && passwd -d hadoop && \
    useradd -mU -d /home/$SPARK_USER -G hadoop $SPARK_USER && passwd -d $SPARK_USER && \
    chown -R $SPARK_USER:hadoop $SPARK_HOME


COPY executejar.sh /

RUN apk add gettext

RUN mkdir -p /opt/docker/tmp

RUN mkdir -p /spark/logs

ADD spark-defaults.conf $SPARK_HOME/spark-defaults.conf

ENV SPARK_LOCAL_DIRS /opt/docker/tmp

RUN chmod +x executejar.sh


## Scratch directories can be passed as volumes
# SPARK_HOME/work directory used on worker for scratch space and job output logs.
# /tmp - Directory to use for "scratch" space in Spark, including map output files and RDDs that get stored on disk.
VOLUME [ "/usr/local/spark/work", "/tmp" ]
RUN chown -R $SPARK_USER:hadoop /tmp

EXPOSE 8081
USER $SPARK_USER:hadoop
CMD ["/executejar.sh"]
