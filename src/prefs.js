import { create } from 'zustand'

export const usePrefs = create((set) => ({
  screenshot: false,
  toggleScreenshot: () => set((state) => ({ screenshot: !(state.screenshot) })),
}))
