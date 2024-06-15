import contentTypes from './mimes.json' with { type: 'json' };
type Ext = keyof typeof contentTypes;

const keys = Object.keys(contentTypes) as Ext[];
keys.sort((a, b) => {
  const mimeA = contentTypes[a];
  const mimeB = contentTypes[b];
  const comp = mimeA.localeCompare(mimeB);
  if (comp === 0) {
    return a.localeCompare(b);
  }
  return comp;
});

const sorted: Partial<Record<Ext, string>> = {};
for (const k of keys) {
  sorted[k] = contentTypes[k];
}

console.log(JSON.stringify(sorted, null, 2));
