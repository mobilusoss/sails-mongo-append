FROM node:22-alpine

RUN apk add --no-cache make

WORKDIR /usr/src/app

ADD package.json package.json
ADD yarn.lock yarn.lock
RUN yarn
ADD . .

CMD ["yarn","test"]
