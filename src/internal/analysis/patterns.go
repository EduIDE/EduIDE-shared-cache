var networkPatterns = []string{
	"java/net/Socket",
	"java/net/ServerSocket",
	"java/net/DatagramSocket",
	"java/net/URL",
	"java/net/HttpURLConnection",
	"java/net/InetAddress",
	"java/net/URLConnection",
}

var execPatterns = []string{
	"java/lang/Runtime",
	"java/lang/ProcessBuilder",
	"java/lang/Process",
}

var reflectionPatterns = []string{
	"java/lang/reflect/Method",
	"java/lang/reflect/Field",
	"java/lang/ClassLoader",
	"sun/misc/Unsafe",
}

var filesystemPatterns = []string{
	"java/io/FileOutputStream",
	"java/io/FileWriter",
	"java/io/RandomAccessFile",
	"java/nio/file/Files",
	"java/nio/channels/FileChannel",
}