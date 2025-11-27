FROM node:18

WORKDIR /app

COPY package.json ./
RUN npm install

COPY . .

CMD ["node", "f1_data_collector.js"]