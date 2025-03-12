from myairflow.send_notify import send_noti

def test_notify():
    msg = "pytest : TOM"
    r = send_noti(msg)
    assert r == 204
