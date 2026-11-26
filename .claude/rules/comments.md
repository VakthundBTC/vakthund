# Rule: comments are concise and explain only what the code does not

Write a comment only when it tells the reader something they cannot see from the code itself:
why something is done, a non-obvious constraint or behaviour, a reference to the docs, or an
open `TBD-n` item.

Do not:

- comment every item, field, variant or line;
- restate what a name or type already says (`/// Connected.` on `Connected`,
  `/// The sender.` on `peer`);
- list the contents of a module or crate in its doc comment: the file structure already shows
  them;
- repeat the docs at length: link or name the section instead.

Keep comments short. Wrap every comment line at 100 characters, so it can be read without
scrolling.
