export const TOOL_OUTPUT_STYLES = {
  text: {
    container: "bg-blue-50 border border-blue-100 p-6 rounded-lg shadow-sm",
    title: "text-blue-700 font-medium text-lg mb-4",
    content: "text-blue-600 space-y-2",
    item: "text-lg"
  },
  card: {
    container: "bg-white border border-gray-200 rounded-xl shadow-sm overflow-hidden",
    header: "p-6 border-b border-gray-100",
    title: "text-xl font-semibold text-gray-900",
    description: "mt-2 text-gray-600",
    content: "p-6",
    details: "prose prose-gray max-w-none",
    detailsKey: "text-sm font-medium text-gray-500 uppercase",
    detailsValue: "mt-1 text-gray-900",
    media: "mt-4 rounded-lg overflow-hidden",
    image: "w-full h-auto object-cover",
    video: "w-full h-full",
    footer: "px-6 py-4 bg-gray-50 border-t border-gray-100",
    footerText: "text-sm text-gray-600",
    footerAction: "text-sm font-medium text-blue-600 hover:text-blue-800"
  },
  image: {
    container: "bg-white border border-gray-100 p-6 rounded-lg shadow-sm",
    title: "text-gray-900 font-medium text-lg mb-4",
    imageWrapper: "relative aspect-video w-full overflow-hidden rounded-lg",
    image: "object-contain w-full h-full",
    description: "mt-4 text-sm text-gray-600"
  },
  video: {
    container: "bg-white border border-gray-100 p-6 rounded-lg shadow-sm",
    title: "text-gray-900 font-medium text-lg mb-4",
    videoWrapper: "relative aspect-video w-full overflow-hidden rounded-lg bg-black",
    video: "w-full h-full",
    description: "mt-4 text-sm text-gray-600"
  },
  pdf: {
    container: "bg-white border border-gray-100 p-6 rounded-lg shadow-sm",
    title: "text-gray-900 font-medium text-lg mb-4",
    pdfWrapper: "relative aspect-[4/3] w-full overflow-hidden rounded-lg bg-gray-50",
    pdf: "w-full h-full",
    description: "mt-4 text-sm text-gray-600",
    actionWrapper: "mt-4 flex justify-end",
    action: "text-sm text-blue-600 hover:text-blue-800 underline"
  }
}; 