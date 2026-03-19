const palette = [
  "#10B981", // emerald-500
  "#F59E0B", // amber-500
  "#8B5CF6", // violet-500
  "#3B82F6", // blue-500
  "#EF4444", // red-500
  "#14B8A6", // teal-500
  "#EC4899", // pink-500
  "#F97316", // orange-500
];

function hashString(input) {
  const str = String(input ?? "");
  let hash = 5381;
  for (let i = 0; i < str.length; i += 1) {
    hash = (hash * 33) ^ str.charCodeAt(i);
  }
  return Math.abs(hash) >>> 0;
}

export default function pickColorFromId(id) {
  const h = hashString(id);
  return palette[h % palette.length];
}

