# LLM authorship hygiene

**Hard constraint.** Any content an agent produces that a human outside the repo may read must not betray its origin. That means release notes, PR descriptions, commit messages, issue comments, and any user-visible documentation.

Internal harness content (rules, skills, CLAUDE.md files, subsystem docs) is held to the same spirit but the bar is looser — prose should still read like a person wrote it, but the mechanical tells below are tolerated only if they genuinely aid clarity.

## Banned mechanical tells

- **Em-dashes (`—`) and en-dashes (`–`) in user-facing text.** Use a hyphen, a comma, parentheses, or a full stop instead. (This rule file itself is internal harness content, so it still uses em-dashes — but anything riding out to a customer does not.)
- **Bullet lists where a sentence would do.** Reserve bullets for genuine enumeration (3+ parallel items). Do not bullet-ify two thoughts to look thorough.
- **Three-item lists where the third item is padding.** "Fast, reliable, and scalable." If the third item adds nothing, drop it.
- **Perfectly parallel sentence openings.** "We improved X. We fixed Y. We added Z." One variation per paragraph, minimum.
- **Excessive headers.** A PR description rarely needs `##` sections. A two-paragraph commit message never does.

## Banned phrases and constructs

Signposting and filler:

- "It's worth noting that…", "Importantly,", "Notably,", "Significantly,"
- "In summary,", "Overall,", "Ultimately,", "To conclude,", "In conclusion,"
- "Moreover,", "Furthermore,", "Additionally,", "That said,", "Having said that,"
- "As mentioned earlier,", "As we discussed,"
- Three-item rhetorical lists ending with an abstract noun: "X, Y, and the future."

Performative openings:

- "Certainly!", "Absolutely!", "Of course!", "Great question!", "I'd be happy to…", "I hope this helps!"
- "Let's dive in", "Let's delve", "Let's explore", "Let's embark"

Marketing vocabulary masquerading as engineering:

- "comprehensive", "robust" (and "robust solution" / "robust implementation"), "seamless", "streamlined", "cutting-edge", "state-of-the-art", "next-generation"
- "leverage" (use "use"), "utilize" (use "use"), "facilitate" (use "help" / "let"), "orchestrate" (use "coordinate" unless you genuinely mean a music conductor)
- "unleash", "elevate", "empower", "supercharge", "turbocharge"
- "delve into", "dive deep", "embark on a journey", "navigate the complexities of"
- "holistic", "synergistic", "paradigm", "ecosystem" (unless you actually mean a software ecosystem)

Hedging that dodges a position:

- "arguably", "potentially", "ostensibly", "effectively" (when you mean "in effect"), "essentially"
- "Some might say…", "There are those who argue…"
- Starting a recommendation with "You might want to consider…" when "Do X" is the actual recommendation.

Sentence-level tells:

- **Uniform sentence length.** Three consecutive sentences of ~20 words each reads as machine prose. Vary deliberately.
- **The sandwich.** Compliment → criticism → compliment in feedback is an obvious AI pattern when the content doesn't warrant it. Direct is better.
- **Bullet inflation.** Turning a two-sentence thought into five bullets to look thorough. One sentence is not a bullet list.
- **"Not only X, but also Y" when "X and Y" would do.**
- **Over-capitalising "Important:"** as a rhetorical device (okay for genuine callouts in docs, not okay at the start of every second paragraph).

Time-and-place clichés:

- "In today's fast-paced world…"
- "In the ever-evolving landscape of…"
- "In an era of rapid technological advancement…"
- "Now more than ever…"

## Banned tone

- Performative enthusiasm. If the change is routine, say it's routine.
- Unearned hedging ("It might be worth considering…"). Either recommend or don't.
- Corporate apology theatre. One honest "sorry, that one is on me" beats three paragraphs.

## What human writing looks like

- Short sentences mixed with longer ones.
- Occasional sentence fragments. For emphasis.
- The writer takes a position. "We picked Konva because react-konva had better TS types in 2024" beats "Konva was selected following a thorough evaluation of the available options."
- Details that only a person who did the work would know. Specific file names, specific bugs, specific dates.
- A tiny bit of personality, when the venue allows it (internal docs yes, customer-facing support reply no).

## How to apply

- Before submitting any PR description, commit message, or customer-facing copy, reread it and remove em-dashes, replace "utilize" with "use", and cut the summary paragraph.
- If you are drafting release notes or customer-facing docs, prefer concrete "what this means for you" over "we are excited to announce".
- When in doubt, ask the user to read it before sending. Humans are better at spotting robot prose than robots are.
