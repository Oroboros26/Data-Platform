import axios from 'axios'

// With Vite proxy, /api → http://localhost:8000/api
const BASE = ''

export const searchEnterprises = (q) =>
  axios.get(`${BASE}/api/search`, { params: { q } }).then(r => r.data)

export const getEnterprise = (num) =>
  axios.get(`${BASE}/api/enterprise/${num}`).then(r => r.data)

export const getStats = () =>
  axios.get(`${BASE}/api/stats`).then(r => r.data)

export const dirigeantsSSE = (num, onData, onDone) => {
  const es = new EventSource(`${BASE}/api/enterprise/${num}/dirigeants`)
  es.onmessage = (e) => {
    try {
      const d = JSON.parse(e.data)
      if (d.status === 'done' || d.status === 'error') { es.close(); onDone(d) }
      else onData(d)
    } catch { /* ignore */ }
  }
  es.onerror = () => { es.close(); onDone({ status: 'error' }) }
  return () => es.close()
}
