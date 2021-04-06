const express = require('express')
const bodyParser = require('body-parser')
const cors = require('cors')

const app = express()
const db = require('./queries')
const port = 3003

app.use(bodyParser.json())
app.use(cors({
  origin: true,
  credentials: true
}))
app.use(
  bodyParser.urlencoded({
    extended: true,
  })
)

app.get('/', (request, response) => {
  response.json({ info: 'Balancer Swaps.Vision Universal Data feed API' })
})

app.post('/data/v1/history', db.getHistory)
app.post('/data/v1/histoday', db.getHistoryDay)
app.post('/data/v1/histohour', db.getHistoryHour)
app.post('/data/v1/histominute', db.getHistoryMinute)
app.get('/data/v1/all/pools/:limit', db.getAllPools)
app.get('/data/v1/all/pairs/:limit', db.getAllPairs)
app.get('/data/v1/all/prices/:limit', db.getAllPrices)
app.get('/data/v1/all/tokens/:limit', db.getAllTokens)
app.post('/data/v1/lastchandle', db.getLastCandle)
app.get('/data/v1/coingecko/:symbol', db.getCoingeckoInfo)
app.get('/data/v1/toptokens/:sortby/:limit', db.getTopTokens)
app.get('/data/v1/toptokensbypools/:limit', db.getTopTokensByPools)
app.get('/data/v1/toptokensraking/:sortby/:limit', db.getTopTokensRanking)
app.get('/data/v1/swapstats/:limit', db.getSwapsStats)

app.listen(port, () => {
  console.log(`Swaps.Vision REST Server running on port ${port}.`)
})