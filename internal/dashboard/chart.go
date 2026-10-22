package dashboard

import (
	"fmt"
	"html"
	"html/template"
	"math"
	"strings"
)

// Point is one second on the chart.
type Point struct {
	Allowed  int
	Rejected int
}

type ChartOptions struct {
	Width, Height int
	XLabelEvery   int // label every N points
	XLabel        func(i int) string
	Upto          int    // draw points [0, Upto); -1 = all (used by replay)
	MarkerAt      int    // index of a vertical marker line; -1 = none
	MarkerLabel   string // untrusted (admin input), escaped below
	LimitLine     int    // horizontal "limit" guide; 0 = none
	Empty         string // message when there is nothing to draw
}

// RenderChart draws allowed (green) and rejected (red) lines as SVG.
//
// The SVG is built from numbers and escaped text only, then marked safe for
// the template. Every string that could come from a user goes through
// html.EscapeString first.
func RenderChart(points []Point, o ChartOptions) template.HTML {
	if o.Width == 0 {
		o.Width, o.Height = 760, 260
	}
	if o.Upto < 0 || o.Upto > len(points) {
		o.Upto = len(points)
	}
	const padL, padR, padT, padB = 44, 12, 14, 26
	plotW := float64(o.Width - padL - padR)
	plotH := float64(o.Height - padT - padB)

	maxY := 1
	for _, p := range points {
		maxY = max(maxY, p.Allowed, p.Rejected)
	}
	maxY = max(maxY, o.LimitLine)
	maxY = niceCeil(maxY)

	n := len(points)
	x := func(i int) float64 {
		if n <= 1 {
			return float64(padL)
		}
		return float64(padL) + plotW*float64(i)/float64(n-1)
	}
	y := func(v int) float64 { return float64(padT) + plotH*(1-float64(v)/float64(maxY)) }

	var b strings.Builder
	fmt.Fprintf(&b, `<svg class="chart" viewBox="0 0 %d %d" role="img" aria-label="Allowed and rejected requests per second" xmlns="http://www.w3.org/2000/svg">`, o.Width, o.Height)

	// horizontal grid + y labels
	for i := 0; i <= 4; i++ {
		v := maxY * i / 4
		yy := y(v)
		fmt.Fprintf(&b, `<line class="grid" x1="%d" y1="%.1f" x2="%d" y2="%.1f"/>`, padL, yy, o.Width-padR, yy)
		fmt.Fprintf(&b, `<text class="axis" x="%d" y="%.1f" text-anchor="end">%d</text>`, padL-6, yy+4, v)
	}
	// x labels
	if o.XLabel != nil && o.XLabelEvery > 0 {
		for i := 0; i < n; i += o.XLabelEvery {
			fmt.Fprintf(&b, `<text class="axis" x="%.1f" y="%d" text-anchor="middle">%s</text>`,
				x(i), o.Height-6, html.EscapeString(o.XLabel(i)))
		}
	}
	if o.LimitLine > 0 {
		ly := y(o.LimitLine)
		fmt.Fprintf(&b, `<line class="limit" x1="%d" y1="%.1f" x2="%d" y2="%.1f"/>`, padL, ly, o.Width-padR, ly)
		fmt.Fprintf(&b, `<text class="limit-label" x="%d" y="%.1f" text-anchor="end">limit %d/s</text>`, o.Width-padR, ly-4, o.LimitLine)
	}

	drawn := points[:o.Upto]
	if len(drawn) == 0 && o.Empty != "" {
		fmt.Fprintf(&b, `<text class="empty" x="%d" y="%d" text-anchor="middle">%s</text>`,
			o.Width/2, o.Height/2, html.EscapeString(o.Empty))
	}
	b.WriteString(polyline("rejected", drawn, x, func(p Point) float64 { return y(p.Rejected) }))
	b.WriteString(polyline("allowed", drawn, x, func(p Point) float64 { return y(p.Allowed) }))

	if o.MarkerAt >= 0 && o.MarkerAt < o.Upto {
		mx := x(o.MarkerAt)
		fmt.Fprintf(&b, `<line class="marker" x1="%.1f" y1="%d" x2="%.1f" y2="%d"/>`, mx, padT, mx, o.Height-padB)
		anchor := "start"
		lx := mx + 5
		if mx > float64(o.Width)*0.7 {
			anchor, lx = "end", mx-5
		}
		fmt.Fprintf(&b, `<text class="marker-label" x="%.1f" y="%d" text-anchor="%s">%s</text>`,
			lx, padT+12, anchor, html.EscapeString(o.MarkerLabel))
	}
	b.WriteString(`</svg>`)
	return template.HTML(b.String())
}

func polyline(class string, pts []Point, x func(int) float64, y func(Point) float64) string {
	if len(pts) == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, `<polyline class="%s" points="`, class)
	for i, p := range pts {
		fmt.Fprintf(&b, "%.1f,%.1f ", x(i), y(p))
	}
	b.WriteString(`"/>`)
	return b.String()
}

// niceCeil returns a round axis maximum >= v that splits into 4 round steps
// (step is 1, 2, 2.5 or 5 times a power of ten, always a whole number).
func niceCeil(v int) int {
	need := float64(max(v, 4)) / 4
	exp := math.Pow(10, math.Floor(math.Log10(need)))
	for _, m := range []float64{1, 2, 2.5, 5, 10} {
		step := m * exp
		if step != math.Trunc(step) {
			continue // keep axis labels whole numbers
		}
		if step >= need {
			return int(step) * 4
		}
	}
	return v
}
