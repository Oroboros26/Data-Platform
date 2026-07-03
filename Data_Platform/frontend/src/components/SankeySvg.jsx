const fmt = (v) => {
  if (v == null || v === 0) return '—'
  const abs = Math.abs(v)
  if (abs >= 1_000_000) return `${(v / 1_000_000).toFixed(2)} M€`
  if (abs >= 1_000)     return `${(v / 1_000).toFixed(0)} k€`
  return `${v.toFixed(0)} €`
}

export default function SankeySvg({ yearData }) {
  const ca  = yearData.ca ?? yearData.chiffre_affaires_net
  const mb  = yearData.marge_brute
  const rn  = yearData.resultat_net

  if (!ca && !mb && !rn) {
    return <p className="no-data" style={{ marginTop: 8 }}>Données insuffisantes (modèle abrégé)</p>
  }

  const W = 620, H = 240
  const nodeW = 24
  const maxVal = Math.max(Math.abs(ca || 0), Math.abs(mb || 0), Math.abs(rn || 0)) || 1
  const maxH = 130
  const scale = (v) => v != null ? Math.max(12, Math.abs(v) / maxVal * maxH) : 0
  const cy = H / 2

  const nodes = [
    { x: 60,  label: "Chiffre d'affaires", value: ca, color: '#d0d0d0' },
    { x: 290, label: 'Marge brute',        value: mb, color: '#888888' },
    { x: 520, label: 'Résultat net',       value: rn, color: rn != null && rn >= 0 ? '#f0f0f0' : '#444444' },
  ]

  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: '100%', maxWidth: W, display: 'block' }}>
      {/* Flow bands */}
      {nodes.slice(0, -1).map((n, i) => {
        const next = nodes[i + 1]
        const h1 = scale(n.value), h2 = scale(next.value)
        const y1t = cy - h1 / 2, y1b = cy + h1 / 2
        const y2t = cy - h2 / 2, y2b = cy + h2 / 2
        const x1 = n.x + nodeW, x2 = next.x
        const mid = (x1 + x2) / 2
        return (
          <path key={i}
            d={`M${x1} ${y1t} C${mid} ${y1t},${mid} ${y2t},${x2} ${y2t}
                L${x2} ${y2b} C${mid} ${y2b},${mid} ${y1b},${x1} ${y1b}Z`}
            fill={n.color} opacity="0.12"
          />
        )
      })}

      {/* Node bars + labels */}
      {nodes.map((n, i) => {
        const h = scale(n.value)
        const y = cy - h / 2
        return (
          <g key={i}>
            <rect x={n.x} y={y} width={nodeW} height={h} fill={n.color} rx={5} opacity={0.9} />
            <text x={n.x + nodeW / 2} y={y - 12} textAnchor="middle"
              fill="#6a6a6a" fontSize="10" fontFamily="Segoe UI, system-ui, sans-serif"
              textLength={undefined} style={{ textTransform: 'uppercase', letterSpacing: '0.06em' }}>
              {n.label}
            </text>
            <text x={n.x + nodeW / 2} y={y + h + 20} textAnchor="middle"
              fill={n.color} fontSize="13" fontWeight="300"
              fontFamily="Segoe UI, system-ui, sans-serif">
              {fmt(n.value)}
            </text>
          </g>
        )
      })}
    </svg>
  )
}
