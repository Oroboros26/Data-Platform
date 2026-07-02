import { useState, useEffect, useCallback } from 'react'
import { searchEnterprises, getEnterprise, getStats } from './api'
import FinancialPanel from './components/FinancialPanel'
import DirigeantsPanel from './components/DirigeantsPanel'
import './index.css'

// ── Search view ────────────────────────────────────────────────────────────────
function SearchView({ onSelect }) {
  const [q, setQ] = useState('')
  const [results, setResults] = useState([])
  const [loading, setLoading] = useState(false)
  const [stats, setStats] = useState(null)

  useEffect(() => {
    getStats().then(setStats).catch(() => {})
  }, [])

  const handleSearch = useCallback(async (val) => {
    setQ(val)
    if (val.length < 2) { setResults([]); return }
    setLoading(true)
    try {
      const data = await searchEnterprises(val)
      setResults(data.results || [])
    } catch { setResults([]) }
    finally { setLoading(false) }
  }, [])

  return (
    <>
      <div className="header">
        <h1>BCE/KBO — Secteur Hôtellerie Belgique</h1>
        <div className="subtitle">
          {stats
            ? `${stats.gold.enterprises.toLocaleString()} entreprises analysées · ${stats.scraping.filings.toLocaleString()} dépôts financiers · scraping ${stats.scraping.pct}%`
            : 'Chargement des statistiques...'}
        </div>
      </div>

      <div className="search-wrap">
        <input
          className="search-input"
          type="text"
          placeholder="Recherche par nom d'entreprise ou numéro BCE..."
          value={q}
          onChange={e => handleSearch(e.target.value)}
          autoFocus
        />

        {loading && <p style={{ color: '#64748b', fontSize: '0.85rem', marginTop: 8 }}>Recherche...</p>}

        {results.length > 0 && (
          <div className="results-list">
            {results.map(r => (
              <div key={r.enterprise_number} className="result-card" onClick={() => onSelect(r.enterprise_number)}>
                <div>
                  <div className="rc-name">{r.name || r.enterprise_number}</div>
                  <div className="rc-meta">
                    {r.enterprise_number}
                    {r.form ? ` · ${r.form}` : ''}
                    {r.city ? ` · ${r.city}` : ''}
                    {r.nace?.length ? ` · ${r.nace[0]}` : ''}
                  </div>
                </div>
                <span className={`rc-badge${r.status === 'Actif' ? '' : ' inactive'}`}>
                  {r.status || '?'}
                </span>
              </div>
            ))}
          </div>
        )}

        {!loading && q.length >= 2 && results.length === 0 && (
          <p style={{ color: '#475569', fontSize: '0.85rem', marginTop: 12 }}>Aucun résultat pour « {q} »</p>
        )}
      </div>
    </>
  )
}


// ── Enterprise detail view ─────────────────────────────────────────────────────
function EnterpriseView({ num, onBack }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    setLoading(true); setError(null); setData(null)
    getEnterprise(num)
      .then(setData)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [num])

  if (loading) return <div className="loading">Chargement...</div>
  if (error)   return <div className="error-box">Erreur : {error}</div>
  if (!data)   return null

  const { enterprise: e, financials, scrape_status } = data
  const addr = e.address || {}
  const addrStr = [addr.StreetFR || addr.StreetNL, addr.HouseNumber, addr.Zipcode, addr.MunicipalityFR || addr.MunicipalityNL]
    .filter(Boolean).join(' ')

  const mainActivities  = (e.activities || []).filter(a => a.Classification === 'MAIN')
  const otherActivities = (e.activities || []).filter(a => a.Classification !== 'MAIN')

  return (
    <>
      <button className="back-btn" onClick={onBack}>← Retour à la recherche</button>

      {/* En-tête */}
      <div className="ent-header">
        <h2>{e.PrimaryName || num}</h2>
        <div className="ent-address">{addrStr || '—'}</div>
        <div className="ent-meta">
          <span className={`pill${e.Status === 'AC' ? ' active' : ''}`}>{e.StatusLabel || e.Status}</span>
          {e.JuridicalFormLabel && <span className="pill">{e.JuridicalFormLabel}</span>}
          {e.StartDate && <span className="pill">Depuis {e.StartDate}</span>}
          <span className="pill">N° {num}</span>
          {scrape_status?.status === 'done' && (
            <span className="pill" style={{ borderColor: '#166534', color: '#4ade80' }}>
              {scrape_status.filings_count} dépôts scraped
            </span>
          )}
        </div>
      </div>

      {/* NACE + Dirigeants */}
      <div className="grid-2">
        <div className="panel">
          <h3>Activités NACE</h3>
          <div className="nace-list">
            {mainActivities.map((a, i) => (
              <div key={i} className="nace-item">
                <span className="nace-code">{a.NaceCode}</span>
                <div>
                  <div>{a.NaceLabel || a.NaceCode}</div>
                  <div className="nace-main">Activité principale · {a.NaceVersion}</div>
                </div>
              </div>
            ))}
            {otherActivities.map((a, i) => (
              <div key={i} className="nace-item">
                <span className="nace-code">{a.NaceCode}</span>
                <div>{a.NaceLabel || a.NaceCode}</div>
              </div>
            ))}
            {!e.activities?.length && <p className="no-data">Aucune activité enregistrée</p>}
          </div>
        </div>

        <DirigeantsPanel enterpriseNum={num} />
      </div>

      {/* Financials */}
      <FinancialPanel financials={financials} />
    </>
  )
}


// ── Root App ──────────────────────────────────────────────────────────────────
export default function App() {
  const [selected, setSelected] = useState(null)

  return (
    <div className="app">
      {selected
        ? <EnterpriseView num={selected} onBack={() => setSelected(null)} />
        : <SearchView onSelect={setSelected} />
      }
    </div>
  )
}
