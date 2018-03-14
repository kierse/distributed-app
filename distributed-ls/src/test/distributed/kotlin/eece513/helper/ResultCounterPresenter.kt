import eece513.client.GrepClient

class ResultCounterPresenter : GrepClient.Presenter {
    internal var count = 0
        private set

    override fun displayResponse(response: GrepClient.Server.Response) {
        count++
    }

    override fun displayHelp(msg: String) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}