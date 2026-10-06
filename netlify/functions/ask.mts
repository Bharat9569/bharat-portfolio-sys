// "Ask my AI" — a portfolio assistant that answers questions about Bharat.
// Uses Netlify AI Gateway (Anthropic credentials are injected at runtime) and
// streams plain-text tokens back to the browser.

const MODEL = 'claude-haiku-4-5'
const MAX_TURNS = 10
const MAX_CHARS = 600

const PROFILE = `
Name: Bharat Brahm Singh. Based in Kanpur, India. Open to Software Engineer, Backend and AI roles.
Positioning: Full-stack developer building AI-powered products with modern AI coding agents (Codex, Claude, Cursor).

Education:
- B.Tech, Computer Science & Engineering, Pranveer Singh Institute of Technology, Kanpur (2022–2026), CGPA 7.1/10.
- Class XII (ISC), Sardar Patel Public School, 2021, 79.5%.
- Class X (ICSE), Sardar Patel Public School, 2019, 73.2%.

Stats: 500+ DSA problems solved, 20+ LeetCode contests, 50+ tutors onboarded on PadhaiPass, 3 full-stack projects shipped.

Software development skills: C++, Python, C, JavaScript, SQL, HTML, CSS, React.js, Node.js, Express.js, FastAPI, REST APIs, JWT,
MongoDB, MySQL, Redis, AWS, Docker, Render, Vercel, Git/GitHub, Postman, VS Code, NumPy, Pandas, Matplotlib, Seaborn, scikit-learn.

AI-native development: uses OpenAI Codex, Claude and Cursor as engineering partners; LLM integration (OpenAI API), AI agents,
agentic development, prompt engineering, AI-assisted development, workflow automation, rapid prototyping.
Workflow: Idea -> Prompt/Spec -> AI coding agent -> Implementation -> Test & Debug -> Iterate -> Deploy. Bharat defines the problem,
architecture, requirements and behavior, then uses AI coding agents to accelerate implementation, debugging and iteration.
Currently exploring: RAG, vector databases, Model Context Protocol (MCP), LLM evaluation.

Projects:
- PadhaiPass (2025, live at padhaipass.com): hyperlocal tutor marketplace for Tier-2 India. REST APIs with JWT auth, email verification,
  password reset, role-based authorization; Cloudinary images; Resend email; Cashfree payments; lead management. 50+ tutors and 10+ students.
  Stack: React, Node.js, Express, MongoDB, JWT, Cashfree.
- Sirius.AI (2025): AI interview-prep platform with technical and behavioral mock interviews. Express/MongoDB/JWT APIs; OpenAI API generates
  contextual questions and analyzes responses; personalized feedback after each session.
- Movie Recommendation System (2025): content-based recommendations with cosine similarity, served by FastAPI, React frontend with react-select,
  TMDB API for posters.
- WhatsApp AI Agent: AI-powered WhatsApp workflow built with AI-assisted development, API integrations, backend logic and automation.
- Real-time Multiplayer Game: browser game with room management, server-authoritative game state, player sync, role assignment, responsive UI.
- Instagram Reels Automation: workflows that generate and process simple Instagram Reel content from prompts.

Certifications: IBM "What is Data Science" (Coursera); HTML, CSS, JS, Python (Infosys Springboard).
Coursework: DSA, Operating Systems, DBMS, OOPs, Web Development, Machine Learning.
Contact: email 2k22.cse.32334@gmail.com, phone +91 95699 23598, or the contact form on this page.
`

const SYSTEM = `You are the portfolio assistant on Bharat Brahm Singh's personal website. Visitors are mostly recruiters and engineers.
Answer questions about Bharat using ONLY the profile below. Speak about Bharat in the third person.
Be concise (2–5 sentences or a short list), friendly and professional. Plain text only, no markdown headings or tables; simple "- " bullets are fine.
If something is not in the profile, say you don't have that detail and suggest contacting Bharat via the contact form or email.
Never invent employers, dates, metrics or skills. Don't exaggerate. Politely decline unrelated requests (e.g. writing essays or code for the visitor).

PROFILE:
${PROFILE}`

type Msg = { role: 'user' | 'assistant'; content: string }

export default async (req: Request) => {
  if (req.method !== 'POST') return new Response('Method not allowed', { status: 405 })

  let messages: Msg[]
  try {
    const body = await req.json()
    messages = (Array.isArray(body?.messages) ? body.messages : [])
      .filter((m: Msg) => (m?.role === 'user' || m?.role === 'assistant') && typeof m.content === 'string' && m.content.trim())
      .slice(-MAX_TURNS)
      .map((m: Msg) => ({ role: m.role, content: m.content.slice(0, MAX_CHARS) }))
  } catch {
    return new Response('Invalid JSON', { status: 400 })
  }
  while (messages.length && messages[0].role !== 'user') messages.shift()
  if (!messages.length || messages[messages.length - 1].role !== 'user') {
    return new Response('A user message is required', { status: 400 })
  }

  const baseUrl = (process.env.ANTHROPIC_BASE_URL || 'https://api.anthropic.com').replace(/\/$/, '')
  const upstream = await fetch(`${baseUrl}/v1/messages`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': process.env.ANTHROPIC_API_KEY ?? '',
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({ model: MODEL, max_tokens: 450, system: SYSTEM, messages, stream: true }),
  })

  if (!upstream.ok || !upstream.body) {
    console.error('AI Gateway error', upstream.status, await upstream.text().catch(() => ''))
    return new Response('The assistant is unavailable right now.', { status: 502 })
  }

  // Convert Anthropic SSE events into a plain text stream.
  const decoder = new TextDecoder()
  const encoder = new TextEncoder()
  let buffer = ''
  const stream = upstream.body.pipeThrough(
    new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller) {
        buffer += decoder.decode(chunk, { stream: true })
        const lines = buffer.split('\n')
        buffer = lines.pop() ?? ''
        for (const line of lines) {
          if (!line.startsWith('data:')) continue
          try {
            const evt = JSON.parse(line.slice(5).trim())
            if (evt.type === 'content_block_delta' && evt.delta?.type === 'text_delta') {
              controller.enqueue(encoder.encode(evt.delta.text))
            }
          } catch {
            // ignore keep-alive / partial lines
          }
        }
      },
    }),
  )

  return new Response(stream, {
    headers: { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' },
  })
}

export const config = { path: '/api/ask' }
