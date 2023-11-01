import seedrandom from 'seedrandom';
export async function sleep(timeToSleepMs: number) {
  return new Promise((res) => {
    setTimeout(res, timeToSleepMs);
  });
}

export function shuffleArray<T>(arr: T[], seed: number): T[] {
  const rng = seedrandom(seed.toString());
  const arrCopy = [...arr];
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(rng() * (i + 1));
    [arrCopy[i], arrCopy[j]] = [arrCopy[j], arrCopy[i]];
  }
  return arrCopy;
}
