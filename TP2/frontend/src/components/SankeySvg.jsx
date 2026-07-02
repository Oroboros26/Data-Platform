/**
 * Simple Sankey SVG — 3 nodes fixes : CA → Marge brute → Résultat net
 */
const fmt = (v) => {
  if (v == null || v === 0) return '—'
  const abs = Math.abs(v)
  if (abs >= 1_000_000) return `${(v / 1_000_000).toFixed(2)}M €`
  if (abs >= 1_000) return `${(v / 1_000).toFixed(0)}k €`
  return `${v.toFixed(0)} €`
}

export default function SankeySvg({ yearData }) {
  const { ca, marge_brute, resultat_net } = yearData

  if (!ca && !marge_brute && !resultat_net) {
    return <p className="no-data">Données insuffisantes pour le Sankey (modèle abrégé/micro)</p>
  }

  const W = 600, H = 220
  const nodeW = 20, nodeH = 100
  const maxVal = Math.max(Math.abs(ca || 0), Math.abs(marge_brute || 0), Math.abs(resultat_net || 0)) || 1

  const scale = (v) => v != null ? Math.max(8, Math.abs(v) / maxVal * nodeH) : 0

  const nodes = [
    { x: 60,  label: "Chiffre d'affaires", value: ca,           color: '#7dd3fc' },
    { x: 280, label: 'Marge brute',        value: marge_brute,  color: '#a78bfa' },
    { x: 500, label: 'Résultat net',       value: resultat_net, color: resultat_net != null && resultat_net >= 0 ? '#4ade80' : '#f87171' },
  ]

  const cy = H / 2

  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: '100%', maxWidth: W, display: 'block' }}>
      {nodes.map((n, i) => {
        const h = scale(n.value)
        const y = cy - h / 2
        return (
          <g key={i}>
            <rect x={n.x} y={y} width={nodeW} height={h} fill={n.color} rx="4" opacity="0.9" />
            <text x={n.x + nodeW / 2} y={y - 10} textAnchor="middle" fill="#94a3b8" fontSize="11">{n.label}</text>
            <text x={n.x + nodeW / 2} y={y + h + 18} textAnchor="middle" fill={n.color} fontSize="12" fontWeight="700">
              {fmt(n.value)}
            </text>
          </g>
        )
      })}

      {/* Flow lines between nodes */}
      {nodes.slice(0, -1).map((n, i) => {
        const next = nodes[i + 1]
        const h1 = scale(n.value)
        const h2 = scale(next.value)
        const y1t = cy - h1 / 2, y1b = cy + h1 / 2
        const y2t = cy - h2 / 2, y2b = cy + h2 / 2
        const x1 = n.x + nodeW, x2 = next.x
        const mid = (x1 + x2) / 2

        return (
          <path
            key={i}
            d={`M ${x1} ${y1t} C ${mid} ${y1t}, ${mid} ${y2t}, ${x2} ${y2t}
                L ${x2} ${y2b} C ${mid} ${y2b}, ${mid} ${y1b}, ${x1} ${y1b} Z`}
            fill={n.color} opacity="0.15"
          />
        )
      })}
    </svg>
  )
}
