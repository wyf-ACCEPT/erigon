ARG TARGET_BASE_VERSION="bullseye-20240701"

FROM debian:${TARGET_BASE_VERSION}

ARG GOLANG_BUILDER_VERSION \
    BUILD_DST \
    BUILD_TIMESTAMP \
    VCS_REF_SHORT \
    VCS_REF_LONG \
    VERSION \
    USER=erigon \
    GROUP=erigon

LABEL BUILDER_IMAGE_INFO=golang:${GOLANG_BUILDER_VERSION} \
    MAINTAINER="https://github.com/erigontech/erigon" \
    org.label-schema.build-date=${BUILD_TIMESTAMP} \
    org.label-schema.description="Erigon Ethereum Client" \
    org.label-schema.name="Erigon" \
    org.label-schema.schema-version="1.0" \
    org.label-schema.url="https://torquem.ch" \
    org.label-schema.vcs-ref-short=${VCS_REF_SHORT} \
    org.label-schema.vcs-ref=${VCS_REF_LONG} \
    org.label-schema.vcs-url="https://github.com/erigontech/erigon.git" \
    org.label-schema.vendor="Torquem" \
    org.label-schema.version=${VERSION}

COPY ${BUILD_DST}/* /usr/local/bin/

RUN groupadd ${GROUP} && \
    useradd -d /home/${USER} -g ${GROUP} -m ${USER}

VOLUME [ "/home/${USER}" ]
WORKDIR /home/${USER}

USER ${USER}

EXPOSE 8545 \
       8551 \
       8546 \
       30303 \
       30303/udp \
       42069 \
       42069/udp \
       8080 \
       9090 \
       6060

ENTRYPOINT [ "/usr/local/bin/erigon" ]