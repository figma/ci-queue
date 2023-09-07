export async function sleep(timeToSleepMs: number) {
  return new Promise((res) => {
    setTimeout(res, timeToSleepMs);
  });
}
