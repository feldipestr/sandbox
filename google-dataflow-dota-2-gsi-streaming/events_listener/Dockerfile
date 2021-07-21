FROM node:alpine

RUN apk add --no-cache --virtual .gyp \
        python \
        make \
        g++ \
    && npm install \
    && apk del .gyp

WORKDIR /app
COPY package*.json ./
RUN npm install

COPY . .
EXPOSE 80

COPY google-cloud-pubsub-publisher-credentials.json /opt/google-cloud-pubsub-publisher-credentials.json
ENV GOOGLE_APPLICATION_CREDENTIALS /opt/google-cloud-pubsub-publisher-credentials.json

CMD [ "node", "index.js" ]
