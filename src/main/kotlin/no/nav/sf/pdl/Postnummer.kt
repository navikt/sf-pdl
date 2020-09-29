package no.nav.sf.pdl

internal const val NOT_FOUND_IN_REGISTER = "<NOT_FOUND_IN_REGISTER>"
object PostnummerService {
    private const val resourcePath = "/postnummerregister.txt"

    private val postnummerTable: Map<String, PostnummerResponse> =
        PostnummerService::class.java
            .getResourceAsStream(resourcePath)
            .bufferedReader()
            .lineSequence()
            .map { line ->
                line.split("\t")
                    .dropLastWhile(String::isEmpty)
                    .toTypedArray()
            }
            .associate {
                it[0] to PostnummerResponse(
                    postnummer = it[0],
                    poststed = it[1],
                    kommuneNr = it[2],
                    kommune = it[3],
                    type = it[4]
                )
            }

    private val kommunenummerTable: Map<String, String> =
            PostnummerService::class.java
                    .getResourceAsStream(resourcePath)
                    .bufferedReader()
                    .lineSequence()
                    .map { line ->
                        line.split("\t")
                                .dropLastWhile(String::isEmpty)
                                .toTypedArray()
                    }
                    .associate {
                        it[2] to it[3]
                    }

    fun getPostnummer(postnummer: String): PostnummerResponse? = postnummerTable[postnummer]

    fun getKommunenummer(kommunenummer: String): String? = kommunenummerTable[kommunenummer]
}

data class PostnummerResponse(
    val postnummer: String,
    val poststed: String,
    val kommuneNr: String,
    val kommune: String,
    val type: String
)
