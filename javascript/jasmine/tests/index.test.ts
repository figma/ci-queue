import { add } from "..";

describe("test-add", () => {
    it('correctly adds 1 and 2', () => {
        expect(add(1, 2)).toEqual(3)
    })

    it('correctly adds 2 and 3', () => {
        expect(add(2, 3)).toEqual(4)
    })
});