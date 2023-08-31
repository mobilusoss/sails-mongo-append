FROM node:16-alpine3.17

RUN apk add --no-cache make

WORKDIR /usr/src/app

ADD package.json package.json
ADD yarn.lock yarn.lock
RUN yarn
ADD . .

CMD ["yarn","test"]
