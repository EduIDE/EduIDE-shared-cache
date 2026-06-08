package analysis

// Forbidden API prefix lists, grouped by category.
// HasPrefix matching is used so e.g. "java/net/Socket" also covers
// SocketInputStream, SocketOutputStream, etc. A trailing slash like "sun/net/"
// catches the entire package subtree.

var networkPatterns = []string{
	"java/net/Socket",
	"java/net/ServerSocket",
	"java/net/DatagramSocket",
	"java/net/MulticastSocket",
	"java/net/URL",
	"java/net/HttpURLConnection",
	"java/net/InetAddress",
	"java/net/InetSocketAddress",
	"java/net/URLConnection",
	"java/nio/channels/SocketChannel",
	"java/nio/channels/ServerSocketChannel",
	"java/nio/channels/DatagramChannel",
	"sun/net/",
}

var execPatterns = []string{
	"java/lang/Runtime",
	"java/lang/ProcessBuilder",
	"java/lang/Process",
}

var reflectionPatterns = []string{
	"java/lang/reflect/Method",
	"java/lang/reflect/Field",
	"java/lang/reflect/Constructor",
	"java/lang/ClassLoader",
	"java/lang/invoke/MethodHandle",
	"sun/misc/Unsafe",
}

var filesystemPatterns = []string{
	"java/io/FileOutputStream",
	"java/io/FileInputStream",
	"java/io/FileWriter",
	"java/io/FileReader",
	"java/io/RandomAccessFile",
	"java/nio/file/Files",
	"java/nio/file/Path",
	"java/nio/channels/FileChannel",
}
