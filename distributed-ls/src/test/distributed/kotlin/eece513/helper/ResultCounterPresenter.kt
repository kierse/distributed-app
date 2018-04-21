import eece513.client.Client

class ResultCounterPresenter : Client.Presenter {
    internal var count = 0
        private set

    override fun displayResponse(response: Client.Server.Response) {
        count++
    }

    override fun displayHelp(msg: String) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}