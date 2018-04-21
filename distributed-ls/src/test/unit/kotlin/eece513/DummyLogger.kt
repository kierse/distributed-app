package eece513

import eece513.common.Logger

class DummyLogger : Logger {
    override fun debug(tag: String, msg: String, vararg args: Any) {}
    override fun info(tag: String, msg: String, vararg args: Any) {}
    override fun warn(tag: String, msg: String, vararg args: Any) {}
    override fun error(tag: String, msg: String, vararg args: Any) {}
}