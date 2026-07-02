import { useState, useEffect } from 'react'
import { dirigeantsSSE } from '../api'

export default function DirigeantsPanel({ enterpriseNum }) {
  const [dirigeants, setDirigeants] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setDirigeants([])
    setLoading(true)
    const cleanup = dirigeantsSSE(
      enterpriseNum,
      (d) => {
        // Skip status messages, only add actual dirigeants
        if (d.nom) setDirigeants(prev => [...prev, d])
      },
      () => setLoading(false)
    )
    return cleanup
  }, [enterpriseNum])

  return (
    <div className="panel">
      <h3>
        {loading && <span className="spinner" />}
        Dirigeants &amp; représentants
        {loading && <span style={{ color: '#64748b', fontWeight: 400 }}> — chargement...</span>}
      </h3>

      {!loading && dirigeants.length === 0 && (
        <p className="no-data">Aucun dirigeant trouvé (source : kbopub.economie.fgov.be)</p>
      )}

      <div className="dirigeants-list">
        {dirigeants.map((d, i) => (
          <div key={i} className="dirigeant-item">
            <div>
              <div className="dirigeant-nom">{d.nom || '—'}</div>
              <div className="dirigeant-meta">{d.fonction || ''}</div>
            </div>
            {d.depuis && <div className="dirigeant-depuis">depuis {d.depuis}</div>}
          </div>
        ))}
      </div>
    </div>
  )
}
